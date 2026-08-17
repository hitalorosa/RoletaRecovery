# -*- coding: utf-8 -*-
"""
Roleta Recovery — Vercel Serverless Function (projeto crm-dashboard)
Endpoint: GET /api/roleta-recovery?key=<CRON_SECRET>

Cron externo bate de 5 em 5 min. Cada chamada, por janela (MSG1/2/3):

1. RESERVA de forma atomica os leads da roleta (Supabase roleta_leads) que ja
   passaram do atraso da MSG e ainda nao foram processados (recovery_msgN_at IS NULL).
   O PATCH com filtro `is.null` garante que duas rodadas sobrepostas NUNCA peguem
   o mesmo lead -> ninguem recebe mensagem duplicada.
2. Cruza com os pedidos recentes da Yampi: quem comprou depois de girar a roleta
   fica de fora (mas continua reservado, pra nao ser reprocessado toda rodada).
3. Pra cada lead restante, via API PUBLICA do Nextags (X-ACCESS-TOKEN):
      POST /contacts                      -> cria o contato (idempotente)
      POST /contacts/{id}/send/{flow_id}  -> DISPARA o flow direto
4. Quem falhar tem a reserva desfeita, pra proxima rodada tentar de novo.
5. Grava metrica em dash.roleta_recovery (Supabase do dashboard).

POR QUE DISPARO DIRETO E NAO ETIQUETA (descoberto 05/08/2026):
A ponte original importava o contato por CSV com a etiqueta "Roleta MSG1",
contando que a regra "Roleta Recovery INPUT" ouvisse o evento e chamasse o flow.
Testado com o numero do Luan: o import aplica a etiqueta mas NAO emite o evento,
nem criando contato novo nem etiquetando contato existente. So emite quando um
humano mexe na etiqueta pelo painel. Por isso aqui nao se usa etiqueta nenhuma:
a ordem de envio e explicita. Nao mexer nisso sem refazer o teste.

Env vars (Vercel / projeto crm-dashboard):
- LEADS_SUPABASE_URL / LEADS_SUPABASE_SERVICE_ROLE   (Supabase dos leads da roleta)
- YAMPI_ALIAS / YAMPI_TOKEN / YAMPI_SECRET
- NEXTAGS_API_AUTO   chave da API publica da conta AUTO (formato 1920660.xxxx)
- NEXTAGS_FLOW_MSG1  id do flow "Roleta MSG1" (MSG2/MSG3 so rodam se a env existir)
- ROLETA_MODE: 'dry-run' (so conta, nao reserva nem envia) ou 'live'
- DASH_SUPABASE_URL / DASH_SUPABASE_SERVICE_ROLE     (metrica do dash)
- CRON_SECRET: se setado, exige ?key=<valor> (ou header X-Cron-Key) na chamada

SQL necessario: ver bloco no fim do arquivo.
"""
from http.server import BaseHTTPRequestHandler
import os, json, re, time, hashlib, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timezone, timedelta

# ============ GRUPO DE CONTROLE ============
# Uma fatia dos leads elegiveis e marcada como processada mas NAO recebe mensagem.
#
# Por que isso existe: quem gira a roleta ja sai com o cupom na tela. Entao todo
# mundo que recebe o recovery e, por definicao, alguem que tem o cupom, e contar
# "quantos da lista compraram" nao prova nada. Nao existe comprador de ROLETA10
# fora da lista.
#
# Com dois grupos identicos, sorteados da mesma fila, e mensagem so pra um, o
# vies de "compraria de qualquer jeito" e o mesmo nos dois e some na subtracao.
# A diferenca entre eles e o ganho real da campanha, e vira um numero unico:
# "quem recebeu comprou X% mais que quem nao recebeu".
CONTROLE_PCT = 20        # fatia do controle, em %


def eh_controle(lead_id):
    """Sorteio ESTAVEL pelo hash do id: o mesmo lead cai sempre no mesmo grupo,
    entao reprocessar ou rodar duas vezes nunca muda a composicao do teste."""
    h = int(hashlib.md5(str(lead_id).encode()).hexdigest()[:8], 16)
    return (h % 100) < CONTROLE_PCT

# ============ CONFIG ============
# (nome, env_do_flow, coluna_de_controle, atraso_min, idade_max_min)
# atraso_min    = so manda depois de X min do cadastro
# idade_max_min = teto de idade; mais velho que isso deixa passar
JANELAS = [
    ('MSG1', 'NEXTAGS_FLOW_MSG1', 'recovery_msg1_at',   15,  360),   # 15min .. 6h
    ('MSG2', 'NEXTAGS_FLOW_MSG2', 'recovery_msg2_at',  180, 1440),   # 3h .. 24h
    ('MSG3', 'NEXTAGS_FLOW_MSG3', 'recovery_msg3_at', 1440, 4320),   # 24h .. 72h
]
TETO_POR_RODADA = 60           # trava de seguranca por rodada
SEGUNDOS_LIMITE = 40           # para antes do timeout da Vercel e devolve o resto
YAMPI_PAGINAS = 3              # 300 pedidos recentes
STATUS_COMPROU = {'paid', 'invoiced', 'shipped', 'delivered', 'completed'}
NEXTAGS_API = 'https://app.nextagsai.com.br/api'


# ============ HELPERS ============
def norm_dt(s):
    """Normaliza data que JA esta em UTC (tudo que vem do Supabase).

    O 'T' vira espaco porque a Yampi usa espaco: sem isso a comparacao de texto
    quebra em silencio, ja que no ASCII o espaco vem antes do 'T' e todo pedido
    do mesmo dia era lido como anterior ao lead."""
    return str(s or '')[:19].replace('T', ' ')


def dt_yampi(s):
    """Converte data da Yampi pra UTC.

    A Yampi devolve {date, timezone: 'America/Sao_Paulo'}, ou seja UTC-3, e o
    Supabase devolve UTC. Comparar direto errava por 3 horas: quem comprava
    dentro de 3h depois de girar a roleta passava como 'nao comprou' e recebia
    a mensagem assim mesmo. Brasil nao tem mais horario de verao desde 2019,
    entao somar 3h fixo e seguro. Descoberto 06/08/2026."""
    s = norm_dt(s)
    if not s:
        return ''
    try:
        return (datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
                + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return s


def norm_phone(ph):
    d = re.sub(r'\D', '', str(ph or ''))
    if not d:
        return ''
    if d.startswith('55') and len(d) >= 12:
        d = d[2:]
    if len(d) >= 11:
        return '55' + d[-11:]
    if len(d) == 10:
        return '55' + d
    return ''


def http_json(url, headers=None, data=None, method='GET', timeout=30):
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return r.status, (json.loads(raw) if raw else None)


def sb_headers(key, extra=None):
    h = {'apikey': key, 'Authorization': f'Bearer {key}',
         'Content-Type': 'application/json', 'Accept': 'application/json'}
    if extra:
        h.update(extra)
    return h


# ---------- passo 1: reserva atomica ----------
def reservar_leads(url, key, col, atraso_min, idade_max_min):
    """PATCH com filtro `col is null`: quem ja foi reservado por outra rodada nao volta.
    Retorna as linhas efetivamente reservadas por ESTA chamada."""
    agora = datetime.now(timezone.utc)
    ate   = agora - timedelta(minutes=atraso_min)      # created_at <= ate
    desde = agora - timedelta(minutes=idade_max_min)   # created_at >= desde
    qs = (f'{col}=is.null'
          f'&created_at=gte.{urllib.parse.quote(desde.isoformat())}'
          f'&created_at=lte.{urllib.parse.quote(ate.isoformat())}'
          f'&select=id,phone,created_at')
    H = sb_headers(key, {'Prefer': 'return=representation'})
    body = json.dumps({col: agora.isoformat()}).encode('utf-8')
    _, rows = http_json(f'{url}/rest/v1/roleta_leads?{qs}', headers=H,
                        data=body, method='PATCH', timeout=45)
    return rows or []


def marcar_enviados(url, key, col, ids):
    """Grava msg1_enviada_em SO em quem o envio voltou OK.

    Existe separado de recovery_msg1_at (que so quer dizer 'processado') porque
    e ele que sustenta a medicao de conversao. Misturar os dois faz o dash contar
    como venda da campanha qualquer cliente que um dia girou a roleta."""
    campo = col.replace('recovery_', '').replace('_at', '') + '_enviada_em'   # msg1_enviada_em
    agora = datetime.now(timezone.utc).isoformat()
    H = sb_headers(key, {'Prefer': 'return=minimal'})
    body = json.dumps({campo: agora}).encode('utf-8')
    for i in range(0, len(ids), 80):
        try:
            http_json(f"{url}/rest/v1/roleta_leads?id=in.({','.join(ids[i:i + 80])})",
                      headers=H, data=body, method='PATCH', timeout=30)
        except Exception:
            pass


def marcar_controle(url, key, ids):
    """Marca quem ficou de fora de proposito. A reserva ja foi feita, entao esses
    leads nao voltam pra fila: eles ficam sem mensagem pelo resto do teste."""
    H = sb_headers(key, {'Prefer': 'return=minimal'})
    body = json.dumps({'controle': True}).encode('utf-8')
    for i in range(0, len(ids), 80):
        try:
            http_json(f"{url}/rest/v1/roleta_leads?id=in.({','.join(ids[i:i + 80])})",
                      headers=H, data=body, method='PATCH', timeout=30)
        except Exception:
            pass


def desfazer_reserva(url, key, col, ids):
    """Devolve os leads pro pool (envio falhou, passou do teto ou acabou o tempo)."""
    if not ids:
        return 0
    H = sb_headers(key, {'Prefer': 'return=minimal'})
    body = json.dumps({col: None}).encode('utf-8')
    desfeitos = 0
    for i in range(0, len(ids), 80):          # chunk pra nao estourar a URL
        lote = ids[i:i + 80]
        try:
            http_json(f"{url}/rest/v1/roleta_leads?id=in.({','.join(lote)})", headers=H,
                      data=body, method='PATCH', timeout=30)
            desfeitos += len(lote)
        except Exception:
            pass
    return desfeitos


# ---------- passo 2: quem ja comprou ----------
def yampi_indice_pedidos(alias, token, secret, paginas=YAMPI_PAGINAS):
    """Puxa os pedidos recentes UMA vez. Devolve (indice_por_telefone, pedidos_pagos).

    indice: {telefone_normalizado: [(data, pago)]}  -> usado pra nao mandar pra quem comprou
    pagos:  [{id, fone_fmt, valor, data}]           -> usado pra medir conversao
            fone_fmt vem no MESMO formato que o Supabase guarda ('(19) 99314-8131'),
            entao o cruzamento com roleta_leads e comparacao direta de texto.
    """
    H = {'User-Token': token, 'User-Secret-Key': secret, 'Accept': 'application/json'}
    base = f'https://api.dooki.com.br/v2/{alias}/orders?include=customer,status&limit=100'
    idx, pagos = {}, []
    for page in range(1, paginas + 1):
        try:
            _, data = http_json(f'{base}&page={page}', headers=H, timeout=25)
        except Exception:
            break
        rows = (data or {}).get('data') or []
        if not rows:
            break
        for o in rows:
            cu = (o.get('customer') or {}).get('data') or {}
            fone = (cu.get('phone') or {})
            fd = fone.get('data') or fone
            phn = norm_phone(fd.get('full_number'))
            cdt = o.get('created_at')
            cstr = dt_yampi((cdt.get('date') if isinstance(cdt, dict) else cdt) or '')
            pago = bool(o.get('authorized')) or \
                ((o.get('status') or {}).get('data', {}).get('alias') in STATUS_COMPROU)
            if phn:
                idx.setdefault(phn, []).append((cstr, pago))
            if pago and fd.get('formated_number'):
                pagos.append({'id': o.get('id'), 'fone_fmt': fd['formated_number'],
                              'valor': float(o.get('value_total') or 0), 'data': cstr})
    return idx, pagos


def medir_conversao(sb_url, sb_key, pagos, estado):
    """Quem recebeu a mensagem e comprou DEPOIS de receber.

    Atribuicao por telefone, nao por UTM nem por cupom: sabemos exatamente quem
    recebeu (recovery_msgN_at) e a que horas. Pedido pago daquele telefone depois
    daquele horario e conversao. Nao depende de UTM sobreviver ao checkout nem
    superestima como o cupom faria (a pessoa pode usar ROLETA10 sem ter recebido).

    Marca d'agua (`ultimo_pedido`) evita contar o mesmo pedido em duas rodadas.
    """
    estado = dict(estado or {})
    marca = estado.get('ultimo_pedido') or ''
    novos = [p for p in pagos if p['data'] > marca]
    if not novos:
        return estado, 0, 0.0

    # ATENCAO: usa msg1_enviada_em, NAO recovery_msg1_at. O segundo significa
    # "processado" e esta preenchido em 300k leads que o backfill carimbou sem
    # mandar nada; usar ele contava como conversao qualquer cliente que um dia
    # girou a roleta e comprou hoje. msg1_enviada_em so e gravado quando o envio
    # volta OK de verdade.
    recebeu = {}
    fones = sorted({p['fone_fmt'] for p in novos if p['fone_fmt']})
    for i in range(0, len(fones), 80):
        lote = ','.join('"' + f.replace('"', '') + '"' for f in fones[i:i + 80])
        filtro = urllib.parse.quote(f'in.({lote})', safe='')
        # se a consulta falhar, NAO avanca a marca d'agua: esses pedidos ficam
        # pra proxima rodada em vez de sumirem sem nunca terem sido contados
        _, rows = http_json(
            f'{sb_url}/rest/v1/roleta_leads?select=phone,msg1_enviada_em'
            f'&msg1_enviada_em=not.is.null&phone={filtro}',
            headers=sb_headers(sb_key), timeout=30)
        for r in rows or []:
            q = norm_dt(r.get('msg1_enviada_em'))
            if q and (r['phone'] not in recebeu or q < recebeu[r['phone']]):
                recebeu[r['phone']] = q

    pedidos = 0
    receita = 0.0
    for p in novos:
        q = recebeu.get(p['fone_fmt'])
        if q and p['data'] > q:          # comprou DEPOIS de receber
            pedidos += 1
            receita += p['valor']
    estado['ultimo_pedido'] = max(p['data'] for p in novos)
    return estado, pedidos, receita


def comprou_depois(idx, phone_norm, desde_iso):
    alvo = norm_dt(desde_iso)
    for cstr, pago in idx.get(phone_norm, []):
        if pago and cstr >= alvo:
            return True
    return False


# ---------- passo 3: Nextags via API publica ----------
def nextags(api_key, path, method='GET', body=None, timeout=25):
    """429 do Nextags nao e falha do lead, e 'espera um pouco'. Espera curta de
    proposito: a funcao morre em 60s, entao e melhor devolver o lead pra fila e
    deixar a proxima rodada pegar do que insistir e estourar o tempo."""
    data = json.dumps(body).encode('utf-8') if body is not None else None
    H = {'X-ACCESS-TOKEN': api_key, 'Content-Type': 'application/json',
         'Accept': 'application/json'}
    for tentativa in range(3):
        try:
            return http_json(f'{NEXTAGS_API}{path}', headers=H, data=data,
                             method=method, timeout=timeout)
        except urllib.error.HTTPError as ex:
            if ex.code != 429 or tentativa == 2:
                raise
            time.sleep(1 + tentativa)


def enviar_flow(api_key, phone_norm, first_name, flow_id):
    """Cria/garante o contato e dispara o flow. Levanta excecao se nao enviar.

    Nao usa etiqueta de proposito: o gatilho por etiqueta nao dispara via API
    (ver nota no cabecalho). Aqui a ordem de envio e explicita.
    """
    _, r = nextags(api_key, '/contacts', 'POST',
                   {'phone': '+' + phone_norm, 'first_name': first_name or ''})
    cid = ((r or {}).get('data') or {}).get('id')
    if not cid:
        raise RuntimeError(f'contato sem id: {str(r)[:120]}')
    _, r2 = nextags(api_key, f'/contacts/{cid}/send/{flow_id}', 'POST')
    if not (r2 or {}).get('success'):
        raise RuntimeError(f'send falhou: {str(r2)[:120]}')
    return cid


# ---------- passo 4: metrica no dash ----------
def gravar_metrica(env, stats):
    url = env.get('DASH_SUPABASE_URL') or env.get('SUPABASE_URL')
    key = env.get('DASH_SUPABASE_SERVICE_ROLE') or env.get('SUPABASE_SERVICE_ROLE')
    if not (url and key):
        return 'sem_env'
    H = sb_headers(key)
    _, cur = http_json(f'{url}/rest/v1/dash?id=eq.1&select=roleta_recovery',
                       headers=H, timeout=15)
    atual = ((cur or [{}])[0] or {}).get('roleta_recovery') or {}
    hoje = datetime.now(timezone.utc).date().isoformat()
    novo = dict(atual)
    for chave in (f'dia_{hoje}', f'mes_{hoje[:7]}'):
        acc = dict(novo.get(chave) or {})
        for j in stats['janelas']:
            if j.get('pulado') or j.get('erro'):
                continue
            m = dict(acc.get(j['msg']) or
                     {'checados': 0, 'ja_comprou': 0, 'enviados': 0, 'falhas': 0})
            # a aba do dash le 'checados'; aqui isso e o total reservado na rodada
            m['checados'] += j.get('reservados', 0)
            for kk in ('ja_comprou', 'enviados', 'falhas'):
                m[kk] += j.get(kk, 0)
            acc[j['msg']] = m
        novo[chave] = acc
    # conversao: quem recebeu e comprou depois (atribuicao por telefone)
    pedidos = receita = 0
    try:
        conv, pedidos, receita = medir_conversao(
            env['LEADS_SUPABASE_URL'].rstrip('/'), env['LEADS_SUPABASE_SERVICE_ROLE'],
            stats.get('_pagos') or [], novo.get('conversao'))
        novo['conversao'] = conv
        # a chave de erro so era escrita, nunca apagada: uma falha isolada de rede
        # ficava no dash pra sempre dizendo que a medicao estava quebrada, mesmo
        # com a rodada seguinte contando pedido normalmente
        novo.pop('conversao_erro', None)
        if pedidos:
            for chave in (f'conv_dia_{hoje}', f'conv_mes_{hoje[:7]}'):
                acc = dict(novo.get(chave) or {'pedidos': 0, 'receita': 0.0})
                acc['pedidos'] += pedidos
                acc['receita'] = round(acc['receita'] + receita, 2)
                novo[chave] = acc
    except Exception as ex:
        novo['conversao_erro'] = str(ex)[:120]

    stats.pop('_pagos', None)          # nao vai pro dash, e grande demais
    stats['conversao_rodada'] = {'pedidos': pedidos, 'receita': round(receita, 2)}
    novo['last_run'] = stats['fim']
    novo['last_mode'] = stats['mode']
    novo['ultima_execucao'] = {'yampi_index': stats.get('yampi_index'),
                               'janelas': stats['janelas'],
                               'conversao': stats['conversao_rodada']}
    H2 = sb_headers(key, {'Prefer': 'resolution=merge-duplicates,return=minimal'})
    http_json(f'{url}/rest/v1/dash?on_conflict=id', headers=H2,
              data=json.dumps({'id': 1, 'roleta_recovery': novo}).encode('utf-8'),
              method='POST', timeout=15)
    return 'ok'


# ============ ROTINA ============
def rodar():
    env = os.environ
    mode = env.get('ROLETA_MODE', 'dry-run').lower()
    dry = (mode != 'live')
    relogio = time.monotonic()

    SB_URL = env['LEADS_SUPABASE_URL'].rstrip('/')
    SB_KEY = env['LEADS_SUPABASE_SERVICE_ROLE']
    NT_KEY = env['NEXTAGS_API_AUTO']

    stats = {'mode': mode, 'inicio': datetime.now(timezone.utc).isoformat(), 'janelas': []}

    idx, pagos = yampi_indice_pedidos(env['YAMPI_ALIAS'], env['YAMPI_TOKEN'], env['YAMPI_SECRET'])
    stats['yampi_index'] = len(idx)
    stats['_pagos'] = pagos

    # Com mais de uma janela ligada, processar sempre na mesma ordem faz a ultima
    # morrer de fome: o orcamento de 40s acaba nas primeiras e a ultima devolve
    # tudo que reservou, toda rodada. Duas defesas:
    #   1. cada janela ativa recebe sua fatia do tempo
    #   2. a ordem gira a cada rodada (pela hora), entao ninguem fica sempre por ultimo
    ativas = [j for j in JANELAS if env.get(j[1])]
    for nome, flow_env, col, atraso, idade_max in JANELAS:
        if not env.get(flow_env):
            stats['janelas'].append({'msg': nome, 'pulado': 'flow nao configurado'})
    if not ativas:
        stats['fim'] = datetime.now(timezone.utc).isoformat()
        return stats
    giro = (datetime.now(timezone.utc).minute // 5) % len(ativas)
    ativas = ativas[giro:] + ativas[:giro]
    fatia = SEGUNDOS_LIMITE / len(ativas)
    stats['ordem'] = [j[0] for j in ativas]

    for pos_j, (nome, flow_env, col, atraso, idade_max) in enumerate(ativas):
        flow_id = env.get(flow_env)
        limite_desta = fatia * (pos_j + 1)
        teto_desta = max(1, TETO_POR_RODADA // len(ativas))

        j = {'msg': nome, 'flow_id': flow_id, 'janela_min': [atraso, idade_max]}
        try:
            if dry:
                # dry-run NAO reserva e NAO envia: so conta quantos entrariam
                agora = datetime.now(timezone.utc)
                qs = (f'{col}=is.null'
                      f'&created_at=gte.{urllib.parse.quote((agora - timedelta(minutes=idade_max)).isoformat())}'
                      f'&created_at=lte.{urllib.parse.quote((agora - timedelta(minutes=atraso)).isoformat())}'
                      f'&select=id,phone,created_at&limit={teto_desta}')
                _, leads = http_json(f'{SB_URL}/rest/v1/roleta_leads?{qs}',
                                     headers=sb_headers(SB_KEY), timeout=45)
                leads = leads or []
            else:
                leads = reservar_leads(SB_URL, SB_KEY, col, atraso, idade_max)
                if len(leads) > teto_desta:
                    leads.sort(key=lambda l: l.get('created_at') or '')
                    excedente = leads[teto_desta:]
                    leads = leads[:teto_desta]
                    desfazer_reserva(SB_URL, SB_KEY, col, [l['id'] for l in excedente])
                    j['adiados_pelo_teto'] = len(excedente)

            j['reservados'] = len(leads)

            fila, controle, comprou, descartados = [], [], 0, 0
            vistos = set()
            for l in leads:
                ph = norm_phone(l.get('phone'))
                if len(ph) < 12 or ph in vistos:
                    descartados += 1
                    continue
                vistos.add(ph)
                if comprou_depois(idx, ph, l.get('created_at') or ''):
                    comprou += 1
                    continue
                # o controle e separado DEPOIS do filtro de comprador, senao os
                # dois grupos nao seriam comparaveis
                if eh_controle(l['id']):
                    controle.append(l['id'])
                    continue
                fila.append((l['id'], ph, l.get('first_name') or ''))

            j['ja_comprou'] = comprou
            j['descartados'] = descartados
            j['controle'] = len(controle)
            if controle and not dry:
                marcar_controle(SB_URL, SB_KEY, controle)

            if dry:
                j['enviados'] = 0
                j['falhas'] = 0
                j['seriam_enviados'] = len(fila)
            else:
                enviados = falhas = 0
                devolver, erros, confirmados = [], [], []
                for pos, (lead_id, ph, nome_lead) in enumerate(fila):
                    if time.monotonic() - relogio > limite_desta:
                        # acabou o tempo: devolve o resto pra proxima rodada
                        devolver += [x[0] for x in fila[pos:]]
                        j['adiados_por_tempo'] = len(fila) - pos
                        break
                    try:
                        enviar_flow(NT_KEY, ph, nome_lead, flow_id)
                        enviados += 1
                        confirmados.append(lead_id)
                    except Exception as ex:
                        falhas += 1
                        devolver.append(lead_id)
                        if len(erros) < 3:
                            erros.append(f'{ph}: {str(ex)[:90]}')
                j['enviados'] = enviados
                j['falhas'] = falhas
                if erros:
                    j['erros'] = erros
                if confirmados:
                    marcar_enviados(SB_URL, SB_KEY, col, confirmados)
                if devolver:
                    j['reserva_desfeita'] = desfazer_reserva(SB_URL, SB_KEY, col, devolver)
        except Exception as ex:
            # falha na reserva (ex: coluna ausente) -> nao envia nada. Falha fechado.
            j['erro'] = f'{type(ex).__name__}: {str(ex)[:200]}'

        stats['janelas'].append(j)

    stats['fim'] = datetime.now(timezone.utc).isoformat()
    stats['duracao_s'] = round(time.monotonic() - relogio, 1)
    try:
        stats['metrica'] = gravar_metrica(env, stats)
    except Exception as ex:
        stats['metrica'] = f'erro: {str(ex)[:150]}'
    return stats


# ============ DASHBOARD (rota ?view=dash) ============
DASH_YAMPI_PAGINAS = 30        # cobertura p/ cruzar conversao no dash
AMOSTRA_MIN_CTRL = 200         # abaixo disso o lift ainda e ruido (ver briefing)


def sb_count(url, key, filtro):
    """Conta linhas sem transferir nenhuma: HEAD + count=exact -> Content-Range '*/N'."""
    H = sb_headers(key, {'Prefer': 'count=exact', 'Range': '0-0'})
    q = f'?{filtro}' if filtro else ''
    req = urllib.request.Request(f'{url}/rest/v1/roleta_leads{q}', method='HEAD', headers=H)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            cr = r.headers.get('Content-Range') or ''
    except Exception:
        return None
    tot = cr.rsplit('/', 1)[-1] if '/' in cr else ''
    return int(tot) if tot.isdigit() else None


def sb_rows(url, key, filtro, limite=8000):
    _, rows = http_json(f'{url}/rest/v1/roleta_leads?{filtro}&limit={limite}',
                        headers=sb_headers(key), timeout=45)
    return rows or []


def dash_dados(env):
    SB_URL = env['LEADS_SUPABASE_URL'].rstrip('/')
    SB_KEY = env['LEADS_SUPABASE_SERVICE_ROLE']
    agora = datetime.now(timezone.utc)

    def desde(dias):
        return urllib.parse.quote((agora - timedelta(days=dias)).isoformat())

    # ---- operacional: contagens por periodo (HEAD, rapido) ----
    periodos = [('Hoje (24h)', 1), ('7 dias', 7), ('30 dias', 30), ('Total', None)]
    tabela = []
    for nome, d in periodos:
        jd = (lambda campo: '' if d is None else f'&{campo}=gte.{desde(d)}')
        tabela.append({
            'nome': nome,
            'leads': sb_count(SB_URL, SB_KEY, '' if d is None else f'created_at=gte.{desde(d)}'),
            'm1':    sb_count(SB_URL, SB_KEY, 'msg1_enviada_em=not.is.null' + jd('msg1_enviada_em')),
            'm2':    sb_count(SB_URL, SB_KEY, 'msg2_enviada_em=not.is.null' + jd('msg2_enviada_em')),
            'm3':    sb_count(SB_URL, SB_KEY, 'msg3_enviada_em=not.is.null' + jd('msg3_enviada_em')),
            'ctrl':  sb_count(SB_URL, SB_KEY, 'controle=is.true' + jd('created_at')),
        })

    # ---- conversao: recebeu vs controle, cruzando Yampi ----
    conv = {'recebidos': 0, 'receb_comprou': 0, 'controle': 0, 'ctrl_comprou': 0,
            'receita': 0.0, 'yampi': 0, 'amostra_ok': False, 'erro': None}
    try:
        idx, _ = yampi_indice_pedidos(env['YAMPI_ALIAS'], env['YAMPI_TOKEN'],
                                      env['YAMPI_SECRET'], paginas=DASH_YAMPI_PAGINAS)
        conv['yampi'] = len(idx)
        recebeu = sb_rows(SB_URL, SB_KEY,
                          'select=phone,msg1_enviada_em&msg1_enviada_em=not.is.null')
        controle = sb_rows(SB_URL, SB_KEY,
                           'select=phone,created_at&controle=is.true')
        conv['recebidos'] = len(recebeu)
        conv['controle'] = len(controle)
        for r in recebeu:
            if comprou_depois(idx, norm_phone(r.get('phone')), r.get('msg1_enviada_em')):
                conv['receb_comprou'] += 1
        for c in controle:
            if comprou_depois(idx, norm_phone(c.get('phone')), c.get('created_at')):
                conv['ctrl_comprou'] += 1
        conv['amostra_ok'] = conv['controle'] >= AMOSTRA_MIN_CTRL and conv['recebidos'] >= 50
    except Exception as ex:
        conv['erro'] = f'{type(ex).__name__}: {str(ex)[:150]}'

    return {
        'mode': env.get('ROLETA_MODE', 'dry-run').lower(),
        'agora': agora.strftime('%d/%m/%Y %H:%M UTC'),
        'tabela': tabela,
        'conv': conv,
    }


def _pct(n, d):
    return (100.0 * n / d) if d else 0.0


def dash_html(env):
    try:
        m = dash_dados(env)
    except Exception as ex:
        return f'<h1>Erro no dash</h1><pre>{type(ex).__name__}: {str(ex)[:300]}</pre>'

    c = m['conv']
    cr = _pct(c['receb_comprou'], c['recebidos'])   # conversao de quem recebeu
    cc = _pct(c['ctrl_comprou'], c['controle'])      # conversao do controle
    lift = cr - cc
    live = m['mode'] == 'live'

    def cel(v):
        return '—' if v is None else f'{v:,}'.replace(',', '.')

    linhas = ''
    for r in m['tabela']:
        linhas += (
            '<tr>'
            f'<td class="p">{r["nome"]}</td>'
            f'<td>{cel(r["leads"])}</td>'
            f'<td>{cel(r["m1"])}</td>'
            f'<td>{cel(r["m2"])}</td>'
            f'<td>{cel(r["m3"])}</td>'
            f'<td>{cel(r["ctrl"])}</td>'
            '</tr>')

    if c['erro']:
        bloco_conv = f'<p class="warn">Não deu pra medir conversão agora: {c["erro"]}</p>'
    else:
        aviso = '' if c['amostra_ok'] else (
            f'<p class="warn">⚠️ Amostra ainda pequena (controle {c["controle"]} / '
            f'mínimo {AMOSTRA_MIN_CTRL}). O ganho abaixo é só uma prévia — '
            'só vira número confiável com mais dados.</p>')
        sinal = '+' if lift >= 0 else ''
        cls = 'good' if lift >= 0 else 'bad'
        bloco_conv = (
            aviso +
            '<div class="cards">'
            f'<div class="card"><span>Recebeu a recovery</span><b>{cr:.1f}%</b>'
            f'<small>{c["receb_comprou"]} de {c["recebidos"]} compraram</small></div>'
            f'<div class="card"><span>Grupo de controle</span><b>{cc:.1f}%</b>'
            f'<small>{c["ctrl_comprou"]} de {c["controle"]} compraram</small></div>'
            f'<div class="card lift {cls}"><span>Ganho da recovery</span>'
            f'<b>{sinal}{lift:.1f} pp</b><small>vs quem não recebeu</small></div>'
            '</div>')

    badge = ('<span class="badge live">LIVE — enviando</span>' if live
             else '<span class="badge dry">DRY-RUN — só medindo</span>')

    return (
        '<!doctype html><html lang="pt-br"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<meta http-equiv="refresh" content="120">'
        '<title>Roleta Recovery — Dry Skin</title><style>'
        ':root{--bg:#0f1115;--card:#181b22;--line:#262b36;--tx:#e7eaf0;--mut:#8a93a6;'
        '--ac:#43b89f;--good:#43b89f;--bad:#e5635e;}'
        '*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);'
        'font-family:-apple-system,Segoe UI,Roboto,sans-serif;padding:24px}'
        '.wrap{max-width:820px;margin:0 auto}h1{font-size:20px;margin:0 0 4px}'
        '.sub{color:var(--mut);font-size:13px;margin-bottom:18px}'
        '.badge{font-size:11px;font-weight:700;padding:3px 9px;border-radius:20px;margin-left:8px}'
        '.badge.live{background:#123a30;color:var(--good)}'
        '.badge.dry{background:#3a2f12;color:#e0b348}'
        'h2{font-size:14px;color:var(--mut);text-transform:uppercase;letter-spacing:.5px;'
        'margin:26px 0 10px;font-weight:700}'
        'table{width:100%;border-collapse:collapse;background:var(--card);border-radius:12px;'
        'overflow:hidden;font-size:14px}'
        'th,td{padding:11px 12px;text-align:right;border-bottom:1px solid var(--line)}'
        'th:first-child,td.p{text-align:left;color:var(--mut)}'
        'th{font-size:11px;text-transform:uppercase;color:var(--mut);letter-spacing:.4px}'
        'tr:last-child td{border-bottom:0}tr:last-child{font-weight:700}'
        '.cards{display:flex;gap:12px;flex-wrap:wrap}'
        '.card{flex:1;min-width:150px;background:var(--card);border:1px solid var(--line);'
        'border-radius:12px;padding:16px}.card span{color:var(--mut);font-size:12px}'
        '.card b{display:block;font-size:28px;margin:6px 0 2px}.card small{color:var(--mut);font-size:12px}'
        '.card.lift.good b{color:var(--good)}.card.lift.bad b{color:var(--bad)}'
        '.warn{background:#3a2f12;color:#e6c565;padding:10px 12px;border-radius:10px;font-size:13px}'
        '.foot{color:var(--mut);font-size:12px;margin-top:22px;line-height:1.5}'
        '</style></head><body><div class="wrap">'
        f'<h1>Roleta Recovery · Dry Skin {badge}</h1>'
        f'<div class="sub">Atualizado {m["agora"]} · atualiza sozinho a cada 2 min · Yampi: {c["yampi"]} pedidos indexados</div>'
        '<h2>Mensagens enviadas</h2>'
        '<table><tr><th>Período</th><th>Leads roleta</th><th>MSG1</th><th>MSG2</th>'
        f'<th>MSG3</th><th>Controle</th></tr>{linhas}</table>'
        '<h2>Conversão · recebeu vs controle</h2>'
        f'{bloco_conv}'
        '<p class="foot">O <b>ganho</b> é quanto quem recebeu a recovery comprou a mais que o '
        'grupo de controle (20% que não recebe nada, de propósito, pra medir o efeito real). '
        'Atribuição por telefone cruzando a Yampi — não depende de UTM nem cupom. '
        f'{"Em DRY-RUN nada é enviado; os números de envio ficam zerados até virar LIVE." if not live else ""}</p>'
        '</div></body></html>')


def autorizado(path, headers):
    segredo = os.environ.get('CRON_SECRET')
    if not segredo:
        return True
    q = urllib.parse.parse_qs(urllib.parse.urlparse(path or '').query)
    if (q.get('key') or [''])[0] == segredo:
        return True
    try:
        return headers.get('X-Cron-Key') == segredo
    except Exception:
        return False


# ============ VERCEL HANDLER ============
class handler(BaseHTTPRequestHandler):
    def _responder(self, code, payload):
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _responder_html(self, code, html):
        body = html.encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if not autorizado(self.path, self.headers):
            return self._responder(401, {'erro': 'nao autorizado'})
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path or '').query)
        if 'dash' in q or (q.get('view') or [''])[0] == 'dash':
            try:
                return self._responder_html(200, dash_html(os.environ))
            except Exception as e:
                return self._responder_html(500, f'<pre>{type(e).__name__}: {e}</pre>')
        try:
            self._responder(200, rodar())
        except Exception as e:
            self._responder(500, {'erro': f'{type(e).__name__}: {e}'})

    def do_POST(self):
        self.do_GET()


# ============ RODAR LOCAL (nao afeta a Vercel) ============
if __name__ == '__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    # O .env do proprio repo vem primeiro e e o unico que existe em qualquer
    # maquina. Os outros dois sao pastas da maquina do Luan e sao ignoradas em
    # silencio quando nao existem, entao isto roda no clone de qualquer um.
    repo_env = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    achou = False
    for envfile in [repo_env,
                    r'C:\Users\nk\Desktop\nk\nextags\.env',
                    r'C:\Users\nk\Desktop\nk\claude-code\yampi\.env']:
        if not os.path.exists(envfile):
            continue
        achou = True
        for line in open(envfile, encoding='utf-8'):
            if '=' in line and not line.strip().startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ.setdefault(k, v)
    if not achou and not os.environ.get('LEADS_SUPABASE_URL'):
        raise SystemExit(
            f'Nenhum .env encontrado. Crie {repo_env} com as variaveis listadas '
            f'em .env.example antes de rodar isto local.')
    os.environ.setdefault('ROLETA_MODE', 'dry-run')   # local NUNCA em live sem querer
    print(json.dumps(rodar(), ensure_ascii=False, indent=2))


# ============================================================================
# SQL — ja aplicado em 05/08/2026, deixado aqui como referencia
#
# [A] Supabase dos LEADS (projeto zhdapckcsibmofworhjj):
#   alter table roleta_leads add column if not exists recovery_msg1_at timestamptz;
#   alter table roleta_leads add column if not exists recovery_msg2_at timestamptz;
#   alter table roleta_leads add column if not exists recovery_msg3_at timestamptz;
#   create index if not exists roleta_leads_msg1_pendentes
#     on roleta_leads (created_at) where recovery_msg1_at is null;
#   -- + os indices equivalentes de msg2/msg3
#   -- backfill (marca os antigos como processados, evita disparo retroativo):
#   update roleta_leads set recovery_msg1_at = now(), recovery_msg2_at = now(),
#          recovery_msg3_at = now() where recovery_msg1_at is null;
#
# [B] Supabase do DASH (projeto bgtxqxrowlgzprlzqnsv):
#   alter table dash add column if not exists roleta_recovery jsonb default '{}'::jsonb;
# ============================================================================
