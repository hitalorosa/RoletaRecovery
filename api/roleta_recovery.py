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
CONTROLE_PCT = 0         # fatia do controle, em % (0 = sem controle, dispara pra todos)


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


def liberar_travados(url, key, col, minutos=30, idade_max_min=4320):
    """Reserva orfa: col preenchido (reservado) mas o *_enviada_em correspondente
    continua nulo passado `minutos`. Acontece se uma excecao no meio do processamento
    da janela impede o codigo de chegar no envio ou no desfazer_reserva -> o lead
    fica preso pra sempre sem receber nada. Libera (col=null) pra ser retentado.

    Restrito a created_at recente (idade_max_min): os ~8900 leads do BACKFILL
    (marcados de proposito na virada pra live, pra nao disparar retroativo) tem
    esse mesmo padrao (col preenchido + enviada_em nulo) mas sao antigos -> sem
    esse filtro essa funcao desmarcava o backfill inteiro a cada rodada."""
    envcol = col.replace('recovery_', '').replace('_at', '') + '_enviada_em'
    limite = urllib.parse.quote((datetime.now(timezone.utc) - timedelta(minutes=minutos)).isoformat())
    desde = urllib.parse.quote((datetime.now(timezone.utc) - timedelta(minutes=idade_max_min)).isoformat())
    H = sb_headers(key, {'Prefer': 'return=representation'})
    body = json.dumps({col: None}).encode('utf-8')
    qs = f'{col}=not.is.null&{col}=lte.{limite}&{envcol}=is.null&created_at=gte.{desde}'
    try:
        _, rows = http_json(f'{url}/rest/v1/roleta_leads?{qs}', headers=H,
                            data=body, method='PATCH', timeout=30)
        return len(rows or [])
    except Exception:
        return 0


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
            if not dry:
                j['liberados_travados'] = liberar_travados(SB_URL, SB_KEY, col, idade_max_min=idade_max)
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
DASH_BUILD = '2026-08-18-20h30'   # muda a cada deploy p/ confirmar visualmente qual versao esta no ar
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


def sb_rows_all(url, key, filtro, pagina=1000):
    """Igual ao sb_rows, mas pagina ate acabar.

    O PostgREST corta em 1000 linhas por resposta, independente do `limit` que a
    gente pede. Quem chamou sb_rows achando que traria tudo recebeu 1000 e nao
    percebeu: era o caso do `recebidos` do dash, que virou denominador da
    conversao (mostrava 1.000 quando o real era 1.224) e base da atribuicao de
    receita (so 1.000 das 1.224 pessoas eram cruzadas com a Yampi).
    """
    out, off = [], 0
    while True:
        _, rows = http_json(f'{url}/rest/v1/roleta_leads?{filtro}&limit={pagina}&offset={off}',
                            headers=sb_headers(key), timeout=45)
        rows = rows or []
        out += rows
        if len(rows) < pagina:
            return out
        off += len(rows)


def dash_dados(env, dia=None):
    SB_URL = env['LEADS_SUPABASE_URL'].rstrip('/')
    SB_KEY = env['LEADS_SUPABASE_SERVICE_ROLE']
    agora = datetime.now(timezone.utc)
    mes = urllib.parse.quote(agora.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat())
    # "hoje" = fuso BR (UTC-3), nao UTC
    hoje_br = urllib.parse.quote((agora - timedelta(hours=3)).replace(
        hour=0, minute=0, second=0, microsecond=0).isoformat())

    d = {
        'mode': env.get('ROLETA_MODE', 'dry-run').lower(),
        'agora': agora.strftime('%d/%m/%Y %H:%M UTC'),
        'erro': None, 'yampi': 0,
    }
    # ---- contagens rapidas (HEAD, nao transfere linhas) ----
    d['leads_mes'] = sb_count(SB_URL, SB_KEY, f'created_at=gte.{mes}')
    d['leads_total'] = sb_count(SB_URL, SB_KEY, '')
    envt, envh = {}, {}
    for i in (1, 2, 3):
        col = f'msg{i}_enviada_em'
        envt[i] = sb_count(SB_URL, SB_KEY, f'{col}=not.is.null') or 0
        # "hoje" = quem GIROU a roleta hoje (created_at), nao quando a msg saiu.
        # Filtrando por col>=hoje contava o backlog dos dias de dry-run inteiro,
        # ja que uma msg de um lead de 2 dias atras podia sair hoje mesmo.
        envh[i] = sb_count(SB_URL, SB_KEY, f'{col}=not.is.null&created_at=gte.{hoje_br}') or 0
    d['env_total'], d['env_hoje'] = envt, envh
    d['msgs_total'] = sum(envt.values())
    d['msgs_hoje'] = sum(envh.values())
    d['controle'] = sb_count(SB_URL, SB_KEY, 'controle=is.true') or 0

    # Custo: cada mensagem e um template de marketing do WhatsApp, cobrado em DOLAR.
    # A receita vem em real, entao sem converter o ROAS sai ~5x maior do que e.
    # Os dois valores sao env var de proposito: preco e cambio mudam sem deploy.
    d['custo_msg_usd'] = float(env.get('CUSTO_MSG_USD') or 0.01)
    d['usd_brl'] = float(env.get('USD_BRL') or 5.15)
    d['gasto'] = d['msgs_total'] * d['custo_msg_usd'] * d['usd_brl']

    # ---- visao do DIA escolhido (fuso BR = UTC-3) ----
    try:
        base = datetime.strptime(dia, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        base = (agora - timedelta(hours=3)).replace(hour=0, minute=0, second=0, microsecond=0)
        dia = base.strftime('%Y-%m-%d')
    ini = base + timedelta(hours=3)          # 00:00 BR = 03:00 UTC
    fim = ini + timedelta(days=1)
    QI = urllib.parse.quote(ini.isoformat()); QF = urllib.parse.quote(fim.isoformat())
    dd = {'data': dia, 'pedidos': 0, 'receita': 0.0, '_receb': {}}
    dd['leads'] = sb_count(SB_URL, SB_KEY, f'created_at=gte.{QI}&created_at=lt.{QF}')
    dd['controle'] = sb_count(SB_URL, SB_KEY, f'controle=is.true&created_at=gte.{QI}&created_at=lt.{QF}')
    envd = {}
    for i in (1, 2, 3):
        col = f'msg{i}_enviada_em'
        envd[i] = sb_count(SB_URL, SB_KEY, f'{col}=gte.{QI}&{col}=lt.{QF}') or 0
        for r in sb_rows(SB_URL, SB_KEY, f'select=phone,{col}&{col}=gte.{QI}&{col}=lt.{QF}'):
            ph = norm_phone(r.get('phone')); t = norm_dt(r.get(col))
            if ph and t and (ph not in dd['_receb'] or t < dd['_receb'][ph]):
                dd['_receb'][ph] = t
    dd['env'] = envd; dd['msgs'] = sum(envd.values())
    d['dia'] = dd

    # ---- atribuicao via Yampi: pedidos + receita recuperada, e controle ----
    ped = {1: 0, 2: 0, 3: 0}
    d.update({'pedidos': 0, 'receita': 0.0, 'recebidos': 0, 'ctrl_comprou': 0})
    try:
        idx, pagos = yampi_indice_pedidos(env['YAMPI_ALIAS'], env['YAMPI_TOKEN'],
                                          env['YAMPI_SECRET'], paginas=DASH_YAMPI_PAGINAS)
        d['yampi'] = len(idx)
        pagos_fone = {}
        for p in pagos:
            pagos_fone.setdefault(norm_phone(p['fone_fmt']), []).append((p['data'], p['valor']))
        # atribuicao do DIA escolhido (recebeu no dia e comprou depois)
        for ph, t in d['dia']['_receb'].items():
            aps = sorted([(x, v) for x, v in pagos_fone.get(ph, []) if x > t])
            if aps:
                d['dia']['pedidos'] += 1
                d['dia']['receita'] += aps[0][1]
        recebidos = sb_rows_all(SB_URL, SB_KEY,
            'select=phone,msg1_enviada_em,msg2_enviada_em,msg3_enviada_em'
            '&or=(msg1_enviada_em.not.is.null,msg2_enviada_em.not.is.null,msg3_enviada_em.not.is.null)')
        d['recebidos'] = len(recebidos)
        for r in recebidos:
            ordens = pagos_fone.get(norm_phone(r.get('phone')), [])
            ts = {}
            for i in (1, 2, 3):
                q = norm_dt(r.get(f'msg{i}_enviada_em'))
                if q:
                    ts[i] = q
                    if any(dt > q for dt, _ in ordens):
                        ped[i] += 1
            if ts:
                cedo = min(ts.values())
                aps = sorted([(dt, v) for dt, v in ordens if dt > cedo])
                if aps:
                    d['pedidos'] += 1
                    d['receita'] += aps[0][1]
        controle = sb_rows(SB_URL, SB_KEY, 'select=phone,created_at&controle=is.true')
        for c in controle:
            if comprou_depois(idx, norm_phone(c.get('phone')), c.get('created_at')):
                d['ctrl_comprou'] += 1
    except Exception as ex:
        d['erro'] = f'{type(ex).__name__}: {str(ex)[:150]}'
    d['ped'] = ped
    # ROAS = receita / gasto. Sem gasto nao existe ROAS (nao e zero, e indefinido).
    d['roas'] = (d['receita'] / d['gasto']) if d['gasto'] else None
    d['dia']['gasto'] = d['dia']['msgs'] * d['custo_msg_usd'] * d['usd_brl']
    d['dia']['roas'] = (d['dia']['receita'] / d['dia']['gasto']) if d['dia']['gasto'] else None
    d['dia'].pop('_receb', None)

    # lista de TODOS os leads que giraram a roleta NO DIA (mascarada — pagina publica)
    d['leads_dia_lista'] = []
    try:
        for r in sb_rows(SB_URL, SB_KEY,
                'select=phone,email,created_at,coupon,msg1_enviada_em,msg2_enviada_em,msg3_enviada_em'
                f'&created_at=gte.{QI}&created_at=lt.{QF}&order=created_at.desc', limite=1000):
            nrec = sum(1 for k in ('msg1_enviada_em', 'msg2_enviada_em', 'msg3_enviada_em') if r.get(k))
            d['leads_dia_lista'].append({
                'phone': _mask_phone(r.get('phone')),
                'email': _mask_email(r.get('email')),
                'data': _dt_br(r.get('created_at')),
                'cupom': r.get('coupon') or '—',
                'status': f'{nrec}/3 msg' if nrec else 'na fila',
            })
    except Exception:
        pass

    return d


def _pct(n, d):
    return (100.0 * n / d) if d else 0.0


def _fmt(n):
    return '—' if n is None else f'{int(n):,}'.replace(',', '.')


def _money(v):
    return 'R$ ' + f'{(v or 0):,.0f}'.replace(',', '.')


def _roas(v):
    return '—' if not v else f'{v:,.1f}x'.replace('.', ',')


def _money2(v):
    """Com centavos. O gasto e da ordem de centavos por mensagem: arredondar pra
    real inteiro faz o gasto de um dia virar 'R$ 4' e some a precisao."""
    return 'R$ ' + f'{(v or 0):,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


def _mask_phone(p):
    dig = re.sub(r'\D', '', str(p or ''))
    if len(dig) < 4:
        return '—'
    ddd = dig[-11:-9] if len(dig) >= 11 else dig[:2]
    return f'({ddd}) ••••-{dig[-4:]}'


def _mask_email(e):
    e = str(e or '')
    if '@' not in e:
        return '—'
    nome, dom = e.split('@', 1)
    return (nome[:2] if len(nome) > 2 else nome[:1]) + '•••@' + dom


def _dt_br(s):
    try:
        t = datetime.fromisoformat(str(s).replace('Z', '+00:00')) - timedelta(hours=3)
        return t.strftime('%d/%m %H:%M')
    except Exception:
        return str(s)[:10]


def dash_html(env, dia=None):
    try:
        d = dash_dados(env, dia)
    except Exception as ex:
        return f'<h1>Erro no dash</h1><pre>{type(ex).__name__}: {str(ex)[:300]}</pre>'

    live = d['mode'] == 'live'
    ped, rec = d['pedidos'], d['receita']
    ticket = (rec / ped) if ped else 0
    recb = d.get('recebidos', 0)
    conv_r = _pct(ped, recb)
    conv_c = _pct(d.get('ctrl_comprou', 0), d['controle'])
    lift = conv_r - conv_c
    amostra_ok = d['controle'] >= AMOSTRA_MIN_CTRL and recb >= 50

    status = ('<span class="pill on">● No ar</span>' if live
              else '<span class="pill off">● Dry-run</span>')
    sgl = '+' if lift >= 0 else ''
    liftcls = 'up' if lift >= 0 else 'down'

    def kpi(label, valor, sub, cls=''):
        return (f'<div class="kpi {cls}"><span class="lbl">{label}</span>'
                f'<b class="val">{valor}</b><span class="sub">{sub}</span></div>')

    kpis = (
        kpi('RECEITA RECUPERADA', _money(rec), f'{_fmt(ped)} pedidos atribuídos', 'accent') +
        kpi('PEDIDOS RECUPERADOS', _fmt(ped), f'de {_fmt(recb)} que receberam') +
        kpi('TICKET MÉDIO', _money(ticket), 'por pedido recuperado') +
        kpi('CONVERSÃO', f'{conv_r:.1f}%', f'{_fmt(ped)} de {_fmt(recb)} compraram', 'accent') +
        kpi('GASTO', _money2(d['gasto']),
            f"{_fmt(d['msgs_total'])} msgs × US$ {d['custo_msg_usd']:.2f} "
            f"(dólar a {_money2(d['usd_brl'])})") +
        kpi('ROAS', _roas(d.get('roas')), 'receita ÷ gasto', 'accent')
    )

    metr = (
        kpi('LEADS PROCESSADOS (MÊS)', _fmt(d['leads_mes']), 'giraram a roleta') +
        kpi('MSGS ENVIADAS', _fmt(d['msgs_total']), f'hoje {_fmt(d["msgs_hoje"])}') +
        kpi('LEADS (TOTAL)', _fmt(d['leads_total']), 'desde o início') +
        kpi('YAMPI INDEXADA', _fmt(d['yampi']), 'pedidos p/ atribuição')
    )

    dd = d.get('dia', {})
    envd = dd.get('env', {})
    dia_val = dd.get('data', '')
    dia_cards = (
        kpi('LEADS DO DIA', _fmt(dd.get('leads')), 'giraram a roleta') +
        kpi('MSGS ENVIADAS', _fmt(dd.get('msgs')),
            f"M1 {_fmt(envd.get(1))} · M2 {_fmt(envd.get(2))} · M3 {_fmt(envd.get(3))}") +
        kpi('PEDIDOS RECUPERADOS', _fmt(dd.get('pedidos')), 'receberam e compraram', 'accent') +
        kpi('RECEITA RECUPERADA', _money(dd.get('receita')), 'no dia', 'accent') +
        kpi('GASTO DO DIA', _money2(dd.get('gasto')), f"{_fmt(dd.get('msgs'))} msgs") +
        kpi('ROAS DO DIA', _roas(dd.get('roas')), 'receita ÷ gasto', 'accent')
    )

    janelas = [
        ('MSG 1', '15 min – 6 h após girar', d['env_total'][1], d['env_hoje'][1], d['ped'][1]),
        ('MSG 2', '3 h – 24 h após girar',   d['env_total'][2], d['env_hoje'][2], d['ped'][2]),
        ('MSG 3', '24 h – 72 h após girar',  d['env_total'][3], d['env_hoje'][3], d['ped'][3]),
    ]
    tag = ('<span class="tag on">ATIVA</span>' if live else '<span class="tag off">DRY-RUN</span>')
    msgcards = ''
    for nome, quando, envn, envh, pedn in janelas:
        msgcards += (
            f'<div class="msg"><div class="msg-h"><b>{nome}</b>{tag}</div>'
            f'<div class="when">{quando}</div>'
            f'<div class="mr"><div><span>enviadas</span><em>{_fmt(envn)}</em>'
            f'<i>hoje {_fmt(envh)}</i></div>'
            f'<div><span>pedidos</span><em>{_fmt(pedn) if pedn else "—"}</em></div></div></div>')

    erro = f'<div class="warn">Não deu pra cruzar a Yampi agora: {d["erro"]}</div>' if d['erro'] else ''
    aviso = '' if (amostra_ok or not live) else (
        f'<div class="warn">Amostra ainda pequena (controle {d["controle"]}/{AMOSTRA_MIN_CTRL}). '
        'O ganho vs controle vira número confiável com mais dados.</div>')
    drynote = ('' if live else
               '<div class="warn">Em <b>dry-run</b>: o robô só mede, não envia. '
               'Receita/pedidos/enviadas ficam zerados até virar <b>live</b>.</div>')

    return (
        '<!doctype html><html lang="pt-br"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<meta http-equiv="refresh" content="120">'
        '<title>Roleta Recovery — Dry Skin</title><style>'
        ':root{--bg:#0a0b0e;--card:#131519;--line:#23262d;--tx:#e9edf3;--mut:#7d8694;'
        '--ac:#43b89f;--up:#43b89f;--down:#e5635e;}'
        '*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);'
        "font-family:-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;padding:28px 22px}"
        '.wrap{max-width:1080px;margin:0 auto}'
        '.top{display:flex;align-items:center;justify-content:space-between;margin-bottom:22px;gap:12px;flex-wrap:wrap}'
        '.brand h1{font-size:19px;margin:0;letter-spacing:.2px}'
        '.brand p{margin:2px 0 0;color:var(--mut);font-size:12px}'
        '.pill{font-size:12px;font-weight:700;padding:6px 12px;border-radius:20px}'
        '.pill.on{background:rgba(67,184,159,.14);color:var(--ac)}'
        '.pill.off{background:rgba(224,179,72,.14);color:#e0b348}'
        '.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}'
        '.grid.g3{grid-template-columns:repeat(3,1fr)}'
        '@media(max-width:820px){.grid{grid-template-columns:repeat(2,1fr)}}'
        '@media(max-width:480px){.grid{grid-template-columns:1fr}}'
        '.kpi{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:18px 18px 16px}'
        '.kpi .lbl{color:var(--mut);font-size:11px;font-weight:700;letter-spacing:.5px}'
        '.kpi .val{display:block;font-size:30px;font-weight:750;margin:8px 0 3px;letter-spacing:-.5px}'
        '.kpi .sub{color:var(--mut);font-size:12px}'
        '.kpi.accent .val{color:var(--ac)}.kpi.up .val{color:var(--up)}.kpi.down .val{color:var(--down)}'
        '.banner{background:var(--card);border:1px solid var(--line);border-radius:14px;'
        'padding:16px 18px;margin:16px 0}'
        '.banner h3{margin:0 0 6px;font-size:15px}.banner h3 .pill{margin-left:8px;vertical-align:middle}'
        '.banner p{margin:3px 0;color:var(--mut);font-size:13px;line-height:1.5}'
        '.sec{color:var(--mut);font-size:11px;font-weight:700;letter-spacing:.6px;'
        'text-transform:uppercase;margin:22px 2px 12px}'
        '.daysec{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px}'
        '.daybar{display:flex;align-items:center;gap:6px}'
        '.daybar button{background:var(--card);border:1px solid var(--line);color:var(--tx);'
        'border-radius:8px;width:32px;height:34px;font-size:17px;cursor:pointer;line-height:1}'
        '.daybar button:hover{border-color:var(--ac)}'
        '.daybar input{background:var(--card);border:1px solid var(--line);color:var(--tx);'
        'border-radius:8px;padding:7px 10px;font-size:13px;color-scheme:dark}'
        '.msgs{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}'
        '@media(max-width:820px){.msgs{grid-template-columns:1fr}}'
        '.msg{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px 18px;'
        'border-left:3px solid var(--ac)}'
        '.msg-h{display:flex;align-items:center;justify-content:space-between}'
        '.msg-h b{font-size:15px}'
        '.tag{font-size:10px;font-weight:800;letter-spacing:.5px;padding:3px 8px;border-radius:6px}'
        '.tag.on{background:rgba(67,184,159,.14);color:var(--ac)}'
        '.tag.off{background:rgba(224,179,72,.14);color:#e0b348}'
        '.when{color:var(--mut);font-size:12px;margin:4px 0 14px}'
        '.mr{display:flex;gap:22px}.mr span{display:block;color:var(--mut);font-size:11px}'
        '.mr em{font-style:normal;font-size:22px;font-weight:750;display:block;margin-top:2px}'
        '.mr i{font-style:normal;color:var(--mut);font-size:11px}'
        '.warn{background:rgba(224,179,72,.10);color:#e6c565;border:1px solid rgba(224,179,72,.25);'
        'padding:11px 14px;border-radius:10px;font-size:13px;margin:14px 0}'
        '.cnote{color:var(--mut);font-size:12px;line-height:1.5;margin:0 2px 12px}'
        '.tblwrap{background:var(--card);border:1px solid var(--line);border-radius:14px;'
        'overflow:auto;max-height:420px}'
        '.tbl{width:100%;border-collapse:collapse;font-size:13px}'
        '.tbl th{position:sticky;top:0;background:var(--card);text-align:left;color:var(--mut);'
        'font-size:11px;letter-spacing:.4px;text-transform:uppercase;padding:11px 14px;'
        'border-bottom:1px solid var(--line)}'
        '.tbl td{padding:10px 14px;border-bottom:1px solid var(--line)}'
        '.tbl tbody tr:last-child td{border-bottom:0}'
        '.tbl tbody tr:hover{background:rgba(255,255,255,.02)}'
        '.pager{display:flex;align-items:center;justify-content:center;gap:14px;margin:12px 0 0}'
        '.pager button{background:var(--card);border:1px solid var(--line);color:var(--tx);'
        'border-radius:8px;width:34px;height:34px;font-size:17px;cursor:pointer;line-height:1}'
        '.pager button:hover{border-color:var(--ac)}'
        '.pager #pginfo{color:var(--mut);font-size:13px;min-width:110px;text-align:center}'
        '.foot{color:var(--mut);font-size:12px;margin-top:24px;line-height:1.5}'
        '</style></head><body><div class="wrap">'
        '<div class="top"><div class="brand"><h1>Roleta Recovery</h1><p>Dry Skin · recuperação via WhatsApp</p></div>'
        f'{status}</div>'
        f'<div class="grid g3">{kpis}</div>'
        '<div class="banner">'
        f'<h3>Roleta Recovery {status}</h3>'
        '<p>Follow-up automático via WhatsApp pra quem gira a roleta e não compra. '
        'A cada 5 min o robô cruza Supabase (leads) × Yampi (compras) e envia via Nextags só quem não converteu.</p>'
        '<p>Pedidos atribuídos por telefone (compra depois de receber) — não depende de UTM '
        'sobreviver até o checkout. Cada pessoa recebe no máximo 3 mensagens e para assim que compra.</p></div>'
        f'{erro}{drynote}'
        f'<div class="grid">{metr}</div>'
        '<div class="sec">As 3 mensagens</div>'
        f'<div class="msgs">{msgcards}</div>'
        '<div class="sec daysec"><span>Visão diária · '
        f'{"/".join(reversed(dia_val.split("-")))}</span>'
        '<span class="daybar"><button type="button" onclick="shift(-1)">‹</button>'
        f'<input type="date" id="dp" value="{dia_val}" '
        "onchange=\"if(this.value)location.search='?dia='+this.value\">"
        '<button type="button" onclick="shift(1)">›</button></span></div>'
        f'<div class="grid g3">{dia_cards}</div>'
        f'<div class="sec">Leads que giraram a roleta · {"/".join(reversed(dia_val.split("-")))}</div>'
        f'<p class="cnote"><b>{len(d.get("leads_dia_lista", []))}</b> leads giraram a roleta nesse dia. '
        '10 por página · dados mascarados (página pública).</p>'
        '<div class="tblwrap"><table class="tbl" id="leadstbl"><thead><tr>'
        '<th>Girou em</th><th>Telefone</th><th>E-mail</th><th>Cupom</th><th>Recovery</th></tr></thead><tbody>'
        + (''.join(
            f'<tr><td>{c["data"]}</td><td>{c["phone"]}</td><td>{c["email"]}</td>'
            f'<td>{c["cupom"]}</td><td>{c["status"]}</td></tr>'
            for c in d.get('leads_dia_lista', []))
           or '<tr><td colspan="5" style="color:var(--mut)">Nenhum lead nesse dia.</td></tr>')
        + '</tbody></table></div>'
        '<div class="pager"><button type="button" onclick="pgm(-1)">‹</button>'
        '<span id="pginfo"></span>'
        '<button type="button" onclick="pgm(1)">›</button></div>'
        f'<p class="foot">Atualizado {d["agora"]} · atualiza sozinho a cada 2 min · '
        f'{_fmt(d["leads_total"])} leads no total desde o início. '
        f'<span style="opacity:.4">build {DASH_BUILD}</span></p>'
        '</div>'
        '<script>function shift(n){var i=document.getElementById("dp");'
        'var d=new Date((i.value||new Date().toISOString().slice(0,10))+"T12:00:00");'
        'd.setDate(d.getDate()+n);location.search="?dia="+d.toISOString().slice(0,10);}'
        'var PG=1,PP=10;function paginar(){var rs=document.querySelectorAll("#leadstbl tbody tr");'
        'var n=rs.length,tp=Math.max(1,Math.ceil(n/PP));if(PG>tp)PG=tp;if(PG<1)PG=1;'
        'rs.forEach(function(r,i){r.style.display=(i>=(PG-1)*PP&&i<PG*PP)?"":"none";});'
        'var el=document.getElementById("pginfo");if(el)el.textContent="Página "+PG+" de "+tp;}'
        'function pgm(n){PG+=n;paginar();}paginar();</script>'
        '</body></html>')


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
    # O painel custa ~13s pra montar (11 contagens no Supabase em sequencia mais o
    # indice da Yampi) e o resultado e igual pra todo mundo. Com `s-maxage` a CDN da
    # Vercel guarda a copia pronta; com `stale-while-revalidate` ela entrega a copia
    # velha NA HORA e revalida por tras — ninguem espera o recalculo.
    CACHE_DASH = 'public, s-maxage=300, stale-while-revalidate=3600'

    def _responder(self, code, payload, extra=None):
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _responder_html(self, code, html):
        body = html.encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Cache-Control', self.CACHE_DASH)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        # Com key valida e SEM pedir dash -> roda a recovery (caminho do cron).
        # Sem key (raiz do site) OU pedindo dash -> serve o painel visual (leitura).
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path or '').query)
        quer_dash = 'dash' in q or (q.get('view') or [''])[0] == 'dash'
        # ?view=json devolve os MESMOS dados do painel em JSON, pro dash de CRM
        # (repo hitalorosa/DASHBOARD) montar a tela com a cara dele. Nao expoe nada
        # a mais que a pagina publica: dash_dados ja mascara telefone/e-mail da
        # lista de leads e descarta os telefones crus da atribuicao.
        quer_json = (q.get('view') or [''])[0] == 'json' or 'json' in q
        if autorizado(self.path, self.headers) and not quer_dash and not quer_json:
            try:
                return self._responder(200, rodar())
            except Exception as e:
                return self._responder(500, {'erro': f'{type(e).__name__}: {e}'})
        dia = (q.get('dia') or [None])[0]
        if dia and not re.match(r'^\d{4}-\d{2}-\d{2}$', dia):
            dia = None

        if quer_json:
            # Token opcional: sem DASH_READ_TOKEN a rota e publica como a pagina.
            # Com ele, so responde a quem mandar ?token= igual.
            esperado = os.environ.get('DASH_READ_TOKEN', '')
            if esperado and (q.get('token') or [''])[0] != esperado:
                return self._responder(401, {'erro': 'token invalido'})
            cors = {'Access-Control-Allow-Origin': '*', 'Cache-Control': self.CACHE_DASH}
            try:
                return self._responder(200, dash_dados(os.environ, dia), cors)
            except Exception as e:
                return self._responder(500, {'erro': f'{type(e).__name__}: {e}'}, cors)

        try:
            self._responder_html(200, dash_html(os.environ, dia))
        except Exception as e:
            self._responder_html(500, f'<pre>{type(e).__name__}: {e}</pre>')

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
