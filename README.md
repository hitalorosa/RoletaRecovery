# Roleta Recovery — Dry Skin

Robô que recupera quem girou a roleta, ganhou cupom (`DRYSKIN10`) e **não comprou**.
Roda como função serverless na Vercel; um cron externo bate de 5 em 5 min. A cada
rodada, por janela (MSG1/2/3), ele: reserva os leads no Supabase, corta na Yampi quem
já comprou, separa o grupo de controle (20%) e dispara o flow direto pelo Nextags.

> O código veio do `crm-dashboard` da Nouê (mesmo robô), adaptado pra Dry Skin:
> `CONTROLE_PCT = 20` e sem `first_name` (a tabela da Dry não tem essa coluna).

## Deploy (Vercel via GitHub)
1. Conecta este repo na Vercel.
2. Em **Settings → Environment Variables**, preenche tudo que está no `.env.example`.
3. Deploy. O endpoint fica em `GET /api/roleta-recovery?key=<CRON_SECRET>`.
4. Cron externo (ex: cron-job.org) batendo nesse endpoint **de 5 em 5 min**.

## Banco (Supabase dos leads) — já aplicado
```sql
alter table roleta_leads add column if not exists recovery_msg1_at timestamptz;
alter table roleta_leads add column if not exists recovery_msg2_at timestamptz;
alter table roleta_leads add column if not exists recovery_msg3_at timestamptz;
alter table roleta_leads add column if not exists msg1_enviada_em  timestamptz;
alter table roleta_leads add column if not exists msg2_enviada_em  timestamptz;
alter table roleta_leads add column if not exists msg3_enviada_em  timestamptz;
alter table roleta_leads add column if not exists controle boolean default false;
create index if not exists roleta_leads_msg1 on roleta_leads (recovery_msg1_at) where recovery_msg1_at is null;
create index if not exists roleta_leads_controle on roleta_leads (controle) where controle = true;
```

### ⚠️ Backfill — rodar UMA vez ANTES do live
Marca os leads que já existem como processados, pra o robô **não disparar retroativo**:
```sql
update roleta_leads
   set recovery_msg1_at = now(), recovery_msg2_at = now(), recovery_msg3_at = now()
 where recovery_msg1_at is null;
```

## Subir com segurança
- **dry-run** (padrão): roda de verdade mas só **conta** quantas mandaria. Zero envio.
- **live**: troca `ROLETA_MODE` pra `live`. Testa com o próprio número (cadastra na
  roleta, espera 15 min da MSG1).

## Janelas
| Msg | atraso | idade máx | env do flow |
|-----|--------|-----------|-------------|
| MSG1 | 15 min | 6 h  | `NEXTAGS_FLOW_MSG1` |
| MSG2 | 3 h    | 24 h | `NEXTAGS_FLOW_MSG2` |
| MSG3 | 24 h   | 72 h | `NEXTAGS_FLOW_MSG3` |
