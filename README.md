# Tech Challenge Fase 3 — Pipeline de Dados AWS (State of Data Brasil)

Documentação técnica das decisões de arquitetura, problemas encontrados e soluções aplicadas durante a construção do pipeline. Este documento serve como registro de rastreabilidade — o objetivo é que qualquer pessoa (incluindo eu, revisando depois) entenda **o porquê** de cada decisão, não só o que foi feito.

## 1. Contexto e escopo

Pipeline de dados na AWS (ambiente AWS Academy Lab) para tratar as 3 últimas edições da pesquisa State of Data Brasil (2023-2024, 2024-2025, 2025-2026) e gerar insights de mercado para uma instituição financeira fictícia.

Serviços obrigatórios do desafio: S3, Glue Jobs/Crawlers, Glue Notebook e/ou Athena, Spark.

## 2. Decisões de arquitetura

### 2.1 Redshift descartado

Cogitado inicialmente, mas abandonado. Motivos:
- Não está no escopo obrigatório do desafio.
- Volume de dados (pesquisas anuais, milhares de linhas) não justifica um cluster dedicado — Athena, sendo serverless, é proporcional ao problema.
- O ambiente AWS Academy Lab tem uma VPC restrita (subnet group com apenas 1 AZ), o que gera erro (`InvalidParameterValue: Availability zone relocation`) ao tentar criar clusters com recursos de alta disponibilidade. Contornável, mas sem necessidade real de contornar.

### 2.2 Camadas Bronze / Silver / Gold

- **Bronze**: dado fiel à fonte, sem nenhuma transformação. Existe para preservar a origem e permitir reprocessamento caso algo dê errado nas etapas seguintes.
- **Silver**: dado tratado, com schema harmonizado entre as 3 edições (ver seção 4).
- **Gold**: agregações prontas para consumo analítico (Athena) e geração de gráficos.

### 2.3 Um crawler por edição (não uma tabela particionada única)

Decisão tomada após confirmar que os 3 anos têm contagens de colunas muito diferentes (399, 403 e 388) — schema drift real, não hipotético. Uma tabela Bronze particionada por ano só faria sentido se o schema fosse idêntico entre partições. Como não é, cada ano vira uma tabela Bronze própria:

- `bronze_ano_2023_2024`
- `bronze_ano_2024_2025`
- `bronze_ano_2025_2026`

A unificação em schema único só acontece na Silver, depois da reconciliação manual/assistida.

### 2.4 Estrutura do S3

```
s3://<bucket>/
├── bronze/state_of_data/ano=2023_2024/
├── bronze/state_of_data/ano=2024_2025/
├── bronze/state_of_data/ano=2025_2026/
├── silver/state_of_data/
└── gold/state_of_data/
```

## 3. Peculiaridades do ambiente AWS Academy Lab (documentadas para não perder tempo revalidando)

- **Erro de Trust Policy em Crawlers** (`Service is unable to assume provided role`): na maioria das vezes resolvido apenas re-selecionando a mesma IAM Role no formulário (sem precisar criar role nova) — indício de token de sessão desatualizado no formulário do console, não de permissão real quebrada.
- **Reset do ambiente**: apaga bucket, database do Glue, crawlers, classifiers e notebooks. Nenhum recurso sobrevive ao reset. Prática adotada: manter uma cópia local (fora da AWS) de todo código validado no notebook, para reconstrução rápida.
- **Atraso de propagação no console**: um crawler pode aparecer como "Succeeded" antes da aba "Tables" refletir a tabela recém-criada. Esperar ~20s e recarregar antes de assumir falha.
- **Sessões de Glue Interactive Session presas** (`AlreadyExistsException` ao reiniciar kernel): resolvido com o comando mágico `%stop_session`, ou manualmente parando a sessão em Glue > Interactive Sessions no console.

## 4. Descoberta 1 — Cabeçalho não reconhecido pelo Glue Crawler

**Sintoma**: mesmo com Classifier CSV customizado configurado (`Has headings`), as tabelas Bronze foram catalogadas com colunas genéricas (`col0`, `col1`...).

**Causa raiz**: os nomes de coluna originais contêm caracteres inválidos como identificador Hive/Athena — começam com número e usam ponto (`.`) e barra (`/`) internamente (ex: `0.a_token`, `1.a.1_faixa_idade`). O Glue provavelmente rejeita internamente esse schema e recua para nomes genéricos.

**Decisão**: não depender do Glue Catalog para capturar nomes de coluna reais. Essa responsabilidade passa a ser do Glue Notebook (Spark), lendo o CSV diretamente do S3.

## 5. Descoberta 2 — Dois formatos de cabeçalho diferentes entre edições

- **2023-2024**: cabeçalho serializado como tupla Python — `('P1_a ', 'Idade')`.
- **2024-2025 e 2025-2026**: cabeçalho como string única, código e descrição concatenados — `1.a_idade`.

O parser (`parse_coluna`) foi escrito para reconhecer os dois formatos (regex `PADRAO_FORMATO_A` e `PADRAO_FORMATO_B`), com fallback documentado para casos de exceção pontuais.

### 5.1 Problemas de parsing resolvidos, em ordem

1. **Apóstrofos internos nas descrições** (ex: `ETL's`) quebravam `ast.literal_eval`. Resolvido trocando para regex tolerante (`re.match` com grupo guloso), em vez de interpretação estrita de código Python.
2. **Escape de aspas duplas não reconhecido pelo Spark** — o padrão CSV (RFC4180) usa `""` para aspa literal escapada, mas o Spark por padrão espera `\`. Corrigido com os parâmetros `quote='"', escape='"', multiLine=True` na leitura.
3. **Um caso de parênteses desbalanceados na fonte** (`SQL Server Integration Services (SSIS)`, coluna `P6_b_16`) — tratado como correção manual pontual e documentada, sem generalizar o regex por causa de 1 caso em ~400.

### 5.2 Dicionário de correções manuais

```python
CORRECOES_MANUAIS = {
    "('P6_b_16 ', 'SQL Server Integration Services (SSIS))": ("P6_b_16", "SQL Server Integration Services (SSIS)")
}
```

Princípio adotado: correções pontuais e não-generalizáveis ficam explícitas num dicionário nomeado, nunca escondidas dentro de lógica genérica — facilita auditoria.

## 6. Descoberta 3 — Schema drift real vs. drift aparente (reindexação de código)

Após normalizar os códigos entre formatos (`normalizar_codigo`: prefixa `P` e troca `.` por `_`, mantendo o padrão de 2023-2024 como base), a comparação inicial por conjuntos (interseção/diferença) trouxe:

| Métrica | Valor |
|---|---|
| Comuns aos 3 anos | 330 |
| Exclusivas 2023-2024 | 8 |
| Exclusivas 2024-2025 | 0 |
| Exclusivas 2025-2026 | 46 |

**Risco identificado**: código de coluna não é uma chave 100% estável entre edições. Quando uma pergunta é removida do *meio* de um bloco, todas as perguntas seguintes do bloco são reindexadas (a letra/número desliza uma posição). Isso faz um mesmo código representar perguntas **diferentes** em anos diferentes — silenciosamente, sem gerar nenhum erro técnico.

Exemplo real encontrado: `P4_d` significa "linguagens de programação" em 2023-2024/2024-2025, mas "bancos de dados" em 2025-2026.

### 6.1 Método de detecção

Comparação de similaridade textual (`difflib.SequenceMatcher`) entre as descrições dos 330 códigos comuns, separando dois padrões:
- Baixa similaridade só entre 2023-2024 e as demais → mudança de estilo de redação (seguro, mesma pergunta).
- Baixa similaridade também entre 2024-2025 e 2025-2026 → reindexação real (precisa correção).

Resultado: **55 casos de reindexação real**, concentrados majoritariamente no bloco `P4` (linguagens/bancos/BI/cloud, deslocados por uma pergunta removida no meio do bloco), e 3 casos isolados nos blocos `P2`/`P3`.

### 6.2 Resolução automática por casamento de descrição

Para os 55 casos, buscou-se a descrição de 2025-2026 dentro do dicionário descrição→código de 2024-2025. 53 dos 55 casos foram resolvidos automaticamente dessa forma.

### 6.3 Casos resolvidos manualmente (2)

- **`P3_g`**: pergunta "motivos para não usar IA/LLM" foi deslocada para `P3_h` em 2025-2026 por causa da inserção de uma pergunta nova no meio do bloco 3. Mapeado `P3_g → P3_h`. O `P3_g` de 2025-2026 passa a representar uma pergunta genuinamente nova ("empresa está conseguindo bons resultados com LLMs"), sem histórico anterior.
- **`P4_c`**: a pergunta "fonte de dado mais usada" (escolha única) foi confirmada como **descontinuada** a partir de 2025-2026 — não existe sucessor. Validado checando se as 8 opções dessa pergunta apareciam em outro lugar de 2025-2026; encontradas apenas associadas a uma pergunta vizinha e sempre estável (`P4_b`, "fontes de dados já utilizadas", que compartilha o mesmo catálogo de opções mas é uma pergunta diferente).

## 7. Decisão de design — nome final de coluna: código técnico vs. descrição legível

Optou-se por usar **descrição legível sanitizada** (ex: `idade`, `faixa_salarial`) como nome final de coluna na Silver, em vez do código técnico (`p1_a`). Motivo: o próprio processo de investigação (seção 6) provou que o código não é uma chave estável nem confiável — ele já se mostrou capaz de mudar de significado entre edições sem gerar erro algum. Descrição legível é autoexplicativa para quem consome a tabela depois, sem depender de um dicionário externo.

### 7.1 Geração dos nomes finais

- `slugify()`: normaliza acentos (via `unicodedata.normalize('NFKD', ...)`), remove caracteres especiais, converte para `snake_case`.
- Prioridade de fonte da descrição: 2025-2026 (mais recente) > 2024-2025 > 2023-2024. A pergunta mais atual define o nome padrão.
- Colisões de nome (duas perguntas diferentes gerando o mesmo slug) são desambiguadas anexando o código de origem ao nome final.

### 7.2 Colisões "fantasma" vs. reais — teste de coocorrência

Detectadas inicialmente 88 colisões de nome. Nem toda colisão é um problema: se dois códigos nunca coexistem no mesmo ano (um só aparece em edições antigas, o outro só na mais recente), é sinal de que são **a mesma pergunta reindexada**, não perguntas diferentes — devem ser mescladas em vez de desambiguadas.

**Critério aplicado**: dois códigos com o mesmo slug, que **nunca aparecem juntos no mesmo ano**, são tratados como a mesma pergunta e mesclados no De-Para. Resultado: 30 grupos mesclados corretamente, 58 mantidos como perguntas legitimamente distintas (coincidência de vocabulário, ex.: opções genéricas como "Não sei informar" repetidas em blocos diferentes).

## 8. Descoberta 4 — Cadeias de reindexação em bloco (mais graves que casos isolados)

Ao tentar unificar os 3 DataFrames (`toDF()` + `unionByName`), dois erros de coluna duplicada revelaram que a reindexação não ocorre só em pontos isolados — ela se propaga em **cadeia**, deslocando várias perguntas consecutivas de uma vez quando uma pergunta é removida do meio de um bloco.

### 8.1 Bloco P2 (forma de trabalho) — cadeia de 4 posições

| Conteúdo | Código antigo (2023-24/2024-25) | Código novo (2025-26) |
|---|---|---|
| Layoff | `P2_q` | `P2_p` |
| Modelo de trabalho atual | `P2_r` | `P2_q` |
| Modelo de trabalho ideal | `P2_s` | `P2_r` |
| Atitude se empresa exigir presencial | `P2_t` | `P2_s` |

Caso não detectado pela varredura automática de similaridade textual porque "modelo de trabalho atual" e "modelo de trabalho ideal" têm alta similaridade de forma (só a palavra final muda), apesar do significado divergente.

### 8.2 Bloco P4 (linguagens/bancos/cloud/BI/IA) — cadeia de 3 posições

Três perguntas foram removidas do meio do bloco P4 entre as edições antigas e 2025-2026 ("fonte de dado mais usada", "linguagens no dia a dia", "linguagem mais usada"), deslocando 8 perguntas subsequentes:

| Conteúdo | Código antigo | Código novo (2025-26) |
|---|---|---|
| Linguagem preferida | `P4_f` | `P4_c` |
| Bancos de dados (dia a dia) | `P4_g` | `P4_d` |
| Cloud (dia a dia) | `P4_h` | `P4_e` |
| Cloud preferida | `P4_i` | `P4_f` |
| Ferramenta de BI (dia a dia) | `P4_j` | `P4_g` |
| Ferramenta de BI preferida | `P4_k` | `P4_h` |
| Tipo de uso de IA generativa | `P4_l` | `P4_i` |
| Usa ChatGPT/Copilot | `P4_m` | `P4_j` |

`P4_c`, `P4_d` e `P4_e` (código antigo) ficam sem sucessor — marcados como descontinuados (sufixo `_descontinuada` no nome canônico), pois o mesmo código já foi reaproveitado em 2025-2026 para uma pergunta de conteúdo diferente.

### 8.3 Princípio de correção — `resolver_canonico` com escopo por ano

A correção definitiva exigiu que o dicionário de tradução de código fosse aplicado **apenas nos anos antigos** (2023-2024 e 2024-2025) — nunca no ano de referência (2025-2026), cujos códigos já estão no formato final. Aplicar a tradução indiscriminadamente em todos os anos causava colisão entre um código nativo do ano mais recente e o resultado da tradução de um código antigo equivalente.

```python
ANOS_ANTIGOS = {"2023_2024", "2024_2025"}

def resolver_canonico(codigo_raw, ano_label):
    cod_norm = normalizar_codigo(codigo_raw)
    if ano_label not in ANOS_ANTIGOS:
        return cod_norm  # 2025-2026 é a referência, nunca traduzir
    if cod_norm in DESCONTINUADOS_COMPLETO:
        return f"{cod_norm}_descontinuada"
    return CORRECOES_ANOS_ANTIGOS.get(cod_norm, cod_norm)
```

### 8.4 Validação preventiva antes da união

Antes de aplicar `toDF()`/`unionByName` nos 3 DataFrames, criou-se uma checagem de nomes de coluna duplicados **por ano**, evitando descobrir colisões só no meio da execução do Spark. Prática recomendada para qualquer transformação em lote com muitas colunas geradas programaticamente.

## 9. Problema técnico — pontuação em nomes de coluna ao renomear

`df.select([df[antigo].alias(novo) ...])`, usando o nome antigo como string, falhou com `UNRESOLVED_COLUMN` quando a descrição bruta continha ponto final (comum em descrições longas de 2023-2024). O Spark interpreta `.` dentro do nome como acesso a campo aninhado.

**Solução**: `df.toDF(*lista_de_nomes_na_mesma_ordem)` — renomeia por posição, nunca precisa interpretar o nome antigo como expressão. Mais robusto para nomes de coluna com caracteres arbitrários.

## 10. Camada Silver — resultado final

- Formato: Parquet, particionado por `ano_pesquisa`.
- Local: `s3://<bucket>/silver/state_of_data/`
- Total de colunas: 432 (431 perguntas canônicas + partição `ano_pesquisa`).
- Total de linhas: 14.005 (5.293 em 2023-2024, 5.217 em 2024-2025, 3.495 em 2025-2026) — validado batendo com a soma exata das 3 origens.
- Colunas ausentes numa edição específica (pergunta que não existia naquele ano) ficam `null` — comportamento correto de `unionByName(allowMissingColumns=True)`, não erro.

**Nota operacional**: o Glue Interactive Session perde todo o estado (variáveis, DataFrames em memória) ao expirar ou reiniciar. Por isso a Silver foi persistida em Parquet assim que consolidada — qualquer sessão nova parte da leitura do Parquet (`spark.read.parquet(...)`), sem precisar reprocessar a Bronze.

## 11. Catalogação da Silver no Glue Data Catalog

Diferente da Bronze, a catalogação da Silver não apresentou nenhum dos problemas de schema enfrentados anteriormente — Parquet carrega o schema embutido no próprio arquivo, então o crawler não precisa inferir nomes a partir de uma linha de texto (não houve necessidade de Classifier customizado).

- Um único crawler apontando para a raiz `s3://<bucket>/silver/state_of_data/` (não por ano) — como os dados já estão particionados por `ano_pesquisa` (schema unificado), o crawler reconhece a partição automaticamente e gera **uma tabela só**: `silver_state_of_data`.
- Validação final via Athena, confirmando a contagem de linhas por ano batendo exatamente com a Silver persistida:

```sql
SELECT ano_pesquisa, COUNT(*) as total
FROM silver_state_of_data
GROUP BY ano_pesquisa;
```

Resultado: 5.293 (2023-2024) / 5.217 (2024-2025) / 3.495 (2025-2026) — pipeline validado de ponta a ponta (S3 Bronze → Glue Notebook/Spark → S3 Silver Parquet particionado → Glue Data Catalog → Athena).

## 12. Tipagem da camada Silver

A Silver, ao ser materializada, herdava tudo como `String` (vindo da leitura de CSV na Bronze). A tipagem exigiu perfilar o conteúdo real de cada coluna — não o nome — antes de decidir o tipo final, porque nomes podem enganar (ex: uma coluna chamada com aparência numérica pode conter texto categórico).

### 12.1 Classificação automática por inspeção de conteúdo

Regra aplicada a cada uma das 431 colunas de pergunta (excluindo a partição `ano_pesquisa`):
- **Binária**: todos os valores não-nulos são `"0"` ou `"1"` → indicador de opção marcada em pergunta de múltipla escolha. 347 colunas.
- **Numérica candidata**: todos os valores não-nulos são dígitos puros. Apenas 1 coluna (`idade`).
- **Categórica/texto**: tudo o mais. 83 colunas nessa primeira passada.

### 12.2 Colunas "pai" de múltipla escolha — redundância identificada

Perguntas de múltipla escolha geram, na exportação bruta, uma coluna "pai" com todas as respostas concatenadas em texto livre (ex: `aspectos_prejudicados`) **além** das colunas "filho" binárias (uma por opção). A informação da coluna pai é 100% redundante com as filhas e, na prática, inutilizável para análise agregada (seria quase uma chave única por combinação de respostas).

**Detecção automática**: para cada coluna binária, extrai-se o código "pai" removendo o sufixo numérico do código (`P4_b_1` → pai `P4_b`). Colunas categóricas cujo código coincide com um pai detectado são marcadas para descarte da análise (mantidas na Silver como registro bruto, mas fora da tipagem/Gold).

Dois casos não capturados pela regra automática, resolvidos manualmente:
- **Sufixo `_descontinuada`**: colunas descontinuadas (ver seção 8.2) quebravam a extração do código pai porque o sufixo não é um número. Corrigido tratando o sufixo separadamente antes de aplicar a regra.
- **Cabeçalhos "órfãos"**: `P7_1` e `P8_3` (só em 2023-2024) usam um código numérico solto que não segue o padrão letra-de-bloco dos seus próprios filhos (`P7_a_1`...`P7_a_10`). Não capturados pelo algoritmo de prefixo; confirmados manualmente por inspeção do conteúdo (mesmo padrão de texto concatenado) e adicionados à lista de descarte.

Total de colunas "pai"/descarte: 33.

### 12.3 Categóricas reais — nominal vs. ordinal

Restaram 53 colunas categóricas genuínas, triadas manualmente em:
- **Nominal** (sem ordem natural): gênero, cor/raça/etnia, PCD, localização (estado/região/UF/país), área de formação, setor, cargo.
- **Ordinal** (ordem importa para análise/gráfico): faixa etária, faixa salarial, nível de senioridade, nível de ensino, porte da empresa (nº funcionários/nº pessoas em dados), tempo de experiência (dados/TI), tempo em busca de oportunidade.
- **Identificador/metadado**, fora de qualquer agregação de negócio: `id`, `token`, `data_hora_envio`.

### 12.4 Qualidade de dados nas faixas ordinais

Ao levantar os valores reais de cada coluna ordinal candidata, dois tipos de inconsistência apareceram:

1. **Erros de digitação pontuais** (1–2 linhas cada): `"de R$ 101/mês a R$ 2.000/mês"` (provável falta de dígito, deveria ser `"de R$ 1.001/mês..."`), `"de R$ 25.001/mês a R$ 3000/mês"` (limite superior menor que o inferior — provável falta de zero), `"de 501 a 100"` (deveria ser `"de 501 a 1.000"`). Confirmado baixo volume (1–2 linhas cada) antes de corrigir. **Decisão**: corrigidos via `df.replace()`, unificando com a faixa correta.
2. **Drift de valor entre edições** (não é erro, é mudança real de design da pesquisa): `tempo_de_experiencia_em_dados` tem tanto `"de 4 a 6 anos"` quanto `"de 5 a 6 anos"` como faixas válidas — indicando que a granularidade da pergunta mudou entre edições. **Decisão**: mantidas como categorias distintas na ordem ordinal, por fidelidade ao que cada respondente realmente viu na tela (evita perda de informação por unificação arbitrária).

### 12.5 Aplicação da tipagem

- Binárias: `"0"`/`"1"` → `boolean`, via `when(col(c) == "1", True).when(col(c) == "0", False).otherwise(None)` — mais seguro que `cast("boolean")` direto, pois trata qualquer valor inesperado como nulo explícito em vez de erro silencioso.
- `idade`: `cast("int")`.
- Ordinais: mantidas como `string`, com a ordem lógica documentada à parte (ver seção 12.6), não embutida como número na própria coluna — preserva o valor original legível.

### 12.6 Dicionário de dados como artefato persistido

Tabela auxiliar em Parquet, com uma linha por coluna (ou uma linha por valor de categoria ordinal, com sua posição de ordem), contendo: nome final, código de origem, descrição, tipo final, categoria semântica (numérica / binária multiescolha / pai multiescolha bruto / categórica ordinal / categórica nominal), e — quando ordinal — o valor da categoria e sua posição de ordem.

Local: `s3://<bucket>/silver/_dicionario_dados/`

Serve dois papéis: documentação viva do schema completo, e insumo funcional para a camada Gold (join direto para ordenar eixos de gráfico sem redeclarar a lista de ordem em cada consulta).

## 13. Incidente — corrupção de escrita por leitura/sobrescrita do mesmo caminho

Ao tentar sobrescrever a Silver (`mode("overwrite")`) a partir de um DataFrame que também tinha sido originalmente lido do mesmo caminho, a escrita falhou (`FileDownloadException` / `FileNotFoundException`) porque o Spark, sendo *lazy*, só materializa as transformações no momento do `write` — nesse ponto, ele tenta ler o arquivo de origem exatamente quando o `overwrite` já começou a apagá-lo.

**Tentativa de correção com `.cache()` + `.count()`** (forçar materialização em memória antes do write) não foi suficiente, pois o `overwrite` anterior já havia apagado parcialmente os arquivos antes de falhar — deixando o caminho em estado inconsistente (alguns arquivos antigos apagados, novos nunca escritos).

**Recuperação**: como a Bronze nunca foi alterada, o `df_silver` foi reconstruído do zero a partir da Bronze (não do caminho quebrado da Silver) e escrito num **novo caminho**, `silver/state_of_data_v2/`, em vez de insistir no caminho antigo. Nenhum dado foi perdido — o `_dicionario_dados/`, gravado em operação separada anterior, sobreviveu intacto.

**Decisão prática**: `state_of_data_v2` foi adotado como caminho definitivo (S3 não tem "rename" nativo — mover exigiria copiar todos os arquivos e depois apagar os antigos, esforço não justificado só por estética de nome). O crawler da Silver foi editado para apontar para o novo caminho.

**Princípio geral para daqui em diante**: nunca usar `mode("overwrite")` no mesmo caminho de onde um DataFrame ainda não materializado se origina. Preferir escrever em caminho novo e só então substituir o antigo, ou garantir materialização completa (`.cache()` + ação) antes de qualquer escrita — mesmo essa segunda opção não é garantia total se uma tentativa anterior já tiver corrompido o destino.

## 15. Camada Gold — abordagem de design

Adotada a estratégia de **tabelas fato curadas** em vez de tabelas pré-agregadas: cada tabela Gold contém uma linha por respondente (não por grupo), com apenas as colunas relevantes ao tema, já limpas e tipadas — sem `GROUP BY` aplicado de antemão. As agregações específicas (contagens, percentuais, cruzamentos) são geradas via consulta Athena no momento de montar cada gráfico, não fixadas na camada Gold.

**Motivo**: na fase de storytelling ainda não se sabe com precisão quais cortes a apresentação executiva vai exigir. Uma tabela pré-agregada trava o formato cedo demais; uma tabela fato curada permite qualquer novo corte apenas com uma query no Athena, sem reprocessar Spark.

Mapeamento das 7 perguntas do desafio para tabelas Gold planejadas:

| Pergunta do desafio | Tabela Gold | Status |
|---|---|---|
| Estrutura do mercado brasileiro de Dados | `gold_perfil_mercado` | Concluída |
| Perfis mais valorizados (remuneração) | `gold_remuneracao_por_perfil` | Pendente |
| Diversidade de gênero | `gold_diversidade` | Pendente |
| Tecnologias com maior adoção | `gold_adocao_tecnologias` | Pendente |
| Adoção de IA e impacto | `gold_adocao_ia` | Pendente |
| Diferenças região/senioridade/modelo de trabalho | reaproveita `gold_perfil_mercado` | — |
| Oportunidades/desafios para investimento | síntese qualitativa na apresentação, não uma tabela | — |

## 16. `gold_perfil_mercado`

Local: `s3://<bucket>/gold/perfil_mercado/`, particionado por `ano_pesquisa`.

Colunas: `cargo_atual`, `cargo_atual_agrupado`, `nivel`, `atua_como_gestor`, `situacao_de_trabalho`, `setor`, `numero_de_funcionarios`, `modelo_de_trabalho_atual`, `modelo_de_trabalho_ideal`, `regiao_onde_mora`, `uf_onde_mora`, `genero`, `cor_raca_etnia`, `pcd`, `faixa_idade`, `nivel_de_ensino`, `area_de_formacao`, `tempo_de_experiencia_em_dados`, `tempo_de_experiencia_em_ti`.

Deliberadamente fora do escopo desta tabela: remuneração e tecnologias (vão para tabelas Gold próprias), identificadores (`id`/`token`/`data_hora_envio`). Sem filtros aplicados na Gold — filtros (ex: só quem está empregado atualmente) são decisão de análise, resolvida na query, não na engenharia.

### 16.1 Descoberta — drift de valor dentro de uma coluna categórica de negócio

Diferente do schema drift já documentado (mudança de nome/código de coluna), aqui a coluna `cargo_atual` sempre existiu de forma estável nos 3 anos, mas o **catálogo de opções de resposta** mudou de composição entre edições:

- **2023-2024**: uma única categoria `"Engenheiro de Dados/Arquiteto de Dados/Data Engineer/Data Architect"`.
- **2024-2025 / 2025-2026**: dividida em duas categorias — `"Engenheiro de Dados/Data Engineer/Data Architect"` e `"Arquiteto de Dados/Data Architect"`.

Sem tratamento, uma comparação direta de "% Engenheiro de Dados" entre anos mostraria uma queda artificial (ex: 17,7% → 16,1%), quando na verdade uma fatia dos profissionais só passou a ser contabilizada sob outro rótulo.

Identificadas também duas categorias exclusivas de 2023-2024 (`"Analista de Inteligência de Mercado/Market Intelligence"`, `"DBA/Administrador de Banco de Dados"`) — descontinuadas nas edições seguintes, sem necessidade de tratamento especial, apenas registro para não gerar confusão.

**Decisão**: preservar a coluna original `cargo_atual` intacta (granularidade correta para perguntas específicas de uma edição) e criar uma coluna derivada `cargo_atual_agrupado`, unificando as duas categorias que se dividiram, destinada especificamente a gráficos de tendência ano a ano onde comparabilidade é o objetivo. Evita tanto a perda de informação (harmonizar destrutivamente) quanto o risco de leitura errada (manter separado só com nota de rodapé, que tende a ser ignorada). Validado: evolução de "Engenheiro de Dados/Arquiteto de Dados" ficou suave (17,7% → 17,3% → 17,2%) após a correção, contra a queda abrupta observada antes dela.

### 16.2 Padrão de ambiente — crawler não aplica "update" de schema de forma confiável

Ao adicionar `cargo_atual_agrupado` e reescrever a tabela, o crawler (configurado corretamente com "Update the table definition in the data catalog") não atualizou o schema da tabela no Catalog, mesmo após re-execução — confirmado comparando o schema real do Parquet (`spark.read.parquet(...).printSchema()`, com a coluna presente) contra o schema do Catalog (sem a coluna). 

**Solução que funcionou de forma confiável**: deletar a tabela inteira no Data Catalog e rodar o crawler do zero, forçando descoberta completa de schema em vez de atualização incremental. Este já é o segundo caso no projeto (o primeiro na Bronze, com o Classifier) em que "deletar e recriar" resolveu algo que "atualizar" não resolveu — registrado como padrão prático recorrente deste ambiente específico (AWS Academy Lab), não necessariamente do Glue em geral.

## 17. `gold_remuneracao_por_perfil`

Local: `s3://<bucket>/gold/remuneracao_por_perfil/`, particionado por `ano_pesquisa`.

Colunas: `faixa_salarial`, `cargo_atual`, `cargo_atual_agrupado`, `nivel`, `atua_como_gestor`, `genero`, `cor_raca_etnia`, `regiao_onde_mora`, `modelo_de_trabalho_atual`, `tempo_de_experiencia_em_dados`, `nivel_de_ensino`, mais a coluna derivada `salario_estimado`.

### 17.1 Estimativa numérica a partir de faixa salarial (premissa documentada)

A pesquisa coleta salário como faixa (texto), não como valor numérico livre. Para permitir cálculo de média/mediana, cada faixa foi mapeada para o **ponto médio do intervalo**:

```python
VALOR_ESTIMADO_FAIXA_SALARIAL = {
    "Menos de R$ 1.000/mês": 750,
    "de R$ 1.001/mês a R$ 2.000/mês": 1500,
    # ... demais faixas fechadas: ponto médio exato
    "Acima de R$ 40.001/mês": 45000,
}
```

Para as duas faixas abertas (sem limite inferior ou superior definido), a estimativa é necessariamente arbitrária: `R$ 750` para "menos de R$ 1.000" (ponto médio entre 0 e 1.000) e `R$ 45.000` para "acima de R$ 40.001" (mantendo o mesmo espaçamento de ~5.000 das faixas anteriores). **Ressalva a carregar para qualquer análise/gráfico derivado**: `salario_estimado` é uma aproximação estatística, não o valor real de cada respondente — apropriado para comparação de tendência entre grupos, não para afirmações de valor exato.

Checkpoint de segurança aplicado: filtragem de linhas com `faixa_salarial` preenchida mas `salario_estimado` nulo (indicaria valor não coberto pelo dicionário) — retornou vazio, confirmando que a limpeza de typos feita na Silver (seção 12.4) cobriu todos os casos.

### 17.2 Reuso de lógica entre tabelas Gold

A coluna `cargo_atual_agrupado` (seção 16.1) é necessária em mais de uma tabela Gold. Centralizada numa função reutilizável (`adicionar_cargo_agrupado(df)`) em vez de duplicar a expressão `when/otherwise` em cada script — evita divergência entre tabelas caso a lógica de agrupamento precise ser ajustada no futuro.

### 17.3 Validação de sanidade

Cruzamento cargo × nível × ano (com corte mínimo de 10 respondentes por grupo, para evitar médias estatisticamente frágeis) mostrou hierarquia salarial coerente em três eixos simultâneos: por senioridade dentro do mesmo cargo, por cargo dentro da mesma senioridade (engenharia acima de análise), e volume de respondentes plausível por cargo (mais comuns têm amostras maiores).

## 18. `gold_diversidade`

Local: `s3://<bucket>/gold/diversidade/`, particionado por `ano_pesquisa`.

**Decisão de escopo**: descartada uma versão inicial mais ampla por redundância — `genero`, `cor_raca_etnia` e `pcd` já existem em `gold_perfil_mercado` e `gold_remuneracao_por_perfil`; cruzamentos com cargo/salário podem ser feitos via `JOIN` no Athena usando `cargo_atual_agrupado`/`nivel`/`ano_pesquisa` como chaves comuns entre as tabelas Gold, sem duplicar dado. Mantida enxuta, focada no que é exclusivo desta tabela: os indicadores de percepção de prejuízo profissional (`sim_devido_a_minha_cor_raca_etnia`, `sim_devido_a_minha_identidade_de_genero`, `sim_devido_ao_fato_de_ser_pcd`), que não aparecem em nenhuma outra tabela Gold.

Colunas: `genero`, `cor_raca_etnia`, `pcd`, `cargo_atual`, `cargo_atual_agrupado`, `nivel`, mais os 3 indicadores de prejuízo percebido.

### 18.1 Validação e achado preliminar

Composição de gênero nos 3 anos: Masculino ~75-77%, Feminino ~22-24%, demais categorias abaixo de 0,5%. Percentual de prejuízo profissional percebido por fator: gênero (15-16%) > raça/etnia (10-11%) > PCD (1,3-2,2%) — hierarquia consistente com o tamanho relativo de cada grupo na amostra.

**Observação preliminar para a fase de storytelling**: participação feminina caiu de forma consistente (não oscilante) ao longo das 3 edições — 24,4% (2023-2024) → 23,5% (2024-2025) → 22,0% (2025-2026). Direção da queda é a mesma nos 3 pontos, mas a magnitude é pequena; decisão sobre destacar como tendência relevante ou tratar como variação amostral normal fica para a etapa de análise/storytelling, com mais contexto de significância.

## 19. `gold_adocao_tecnologias`

Local: `s3://<bucket>/gold/adocao_tecnologias/`, particionado por `ano_pesquisa`.

### 19.1 Técnica: unpivot (formato largo → longo)

As colunas binárias de tecnologia (linguagens, bancos, cloud, BI, ETL...) não cabem de forma útil numa tabela larga — geram uma tabela frágil (nova tecnologia = nova coluna) e queries verbosas. Aplicado unpivot via `stack()` do Spark SQL: cada grupo de ~10-30 colunas binárias (ex: todas as linguagens) vira duas colunas — `tecnologia` (nome) e `usa` (boolean) — com uma linha por combinação respondente×tecnologia.

Estrutura final: `ano_pesquisa`, `cargo_atual_agrupado`, `nivel`, `categoria`, `tecnologia`, `usa`.

### 19.2 Escopo — 8 grupos de tecnologia incluídos

Linguagem de Programação, Banco de Dados, Cloud, Ferramenta de BI, ETL (Engenheiro de Dados), ETL (Analista de Dados), Técnicas/Ferramentas de Ciência de Dados, Ferramentas de Autonomia para Negócio.

Deixado de fora deliberadamente: `fontes_de_dados_dia_a_dia` (tipos de dado trabalhado — ex: imagens, vídeos — é uma dimensão conceitualmente diferente de "tecnologia utilizada", candidata a tratamento futuro separado se necessário).

### 19.3 Reaproveitamento do dicionário de dados persistido

Como as variáveis de mapeamento (`nome_para_codigo`, `colunas_binarias`) só existem em memória durante a sessão em que foram criadas, esta etapa recuperou-as diretamente do `_dicionario_dados` persistido (seção 12.6), em vez de reconstruir todo o histórico de parsing/De-Para — validando na prática o propósito de ter esse artefato salvo.

### 19.4 Bug replicado — cálculo de código-pai não tratava sufixo `_descontinuada`

Mesmo problema já resolvido na seção 12.2 (extração de código pai quebrada pelo sufixo `_descontinuada` em códigos descontinuados), reintroduzido aqui porque a correção não havia sido centralizada — resultou em "nenhum filho encontrado" para o grupo de Linguagem de Programação (código `P4_d`, descontinuado a partir de 2025-2026). Corrigido reaplicando a mesma lógica de tratamento de sufixo antes do `rsplit`. **Lição**: lógica de correção replicada em múltiplos scripts deve ser centralizada em função reutilizável (mesmo princípio já registrado na seção 17.2 para `cargo_atual_agrupado`), não copiada manualmente a cada novo uso.

### 19.5 Comportamento esperado — pergunta descontinuada gera 0%/null, não erro

Para a categoria "Linguagem de Programação" (descontinuada em 2025-2026), os respondentes desse ano têm `usa = null` em todas as linhas dessa categoria — resultado de `unionByName(allowMissingColumns=True)` já herdado da Silver. Validado nas queries: `SUM(CASE WHEN usa THEN 1 ELSE 0 END)` trata `null` como 0, gerando 0% de adoção para 2025-2026 nessa categoria especificamente — comportamento correto, não bug. Qualquer gráfico de evolução de linguagens precisa excluir 2025-2026 ou anotar a descontinuação explicitamente.

### 19.6 Validação de sanidade

Ranking de linguagens em 2024-2025: SQL (60,3%) e Python (56,3%) dominando com folga sobre as demais (R 6,7%, Java 5,6%...) — padrão consistente com o que se espera do mercado de dados brasileiro.

## 20. `gold_adocao_ia`

Local: `s3://<bucket>/gold/adocao_ia/`, particionado por `ano_pesquisa`. Última tabela Gold planejada — a mais trabalhosa das cinco, com três problemas reais descobertos e corrigidos durante a construção.

Estrutura: mesma técnica de unpivot de `gold_adocao_tecnologias` (seção 19), aplicada a 4 grupos — Tipo de Uso de IA (nível empresa), Tipo de Uso (nível individual/produto), Motivo de Não Adoção, Uso Pessoal (ChatGPT/Copilot) — mais colunas de contexto de escolha única (`ai_generativa_e_llm_e_uma_prioridade`, `empresa_esta_conseguindo_ter_bons_resultados_com_llms`).

### 20.1 Bug 1 — pergunta de prioridade de IA é respondida majoritariamente por gestores, que usam `cargo_como_gestor`, não `cargo_atual`

A pesquisa bifurca cargo em dois campos mutuamente exclusivos por desenho: quem não é gestor responde `cargo_atual` (Analista, Cientista de Dados...); quem é gestor responde `cargo_como_gestor` (Team Leader, Head, C-level...). A pergunta `ai_generativa_e_llm_e_uma_prioridade` é majoritariamente respondida por gestores — cruzá-la com `cargo_atual_agrupado` (que só cobre não-gestores) zerava a análise para esse público, mascarando o problema com `0%` em vez de erro visível.

**Correção**: adicionada coluna `cargo_unificado = coalesce(cargo_atual_agrupado, cargo_como_gestor)`. Este achado é específico de perguntas direcionadas a gestores — `gold_perfil_mercado` e `gold_remuneracao_por_perfil` não foram afetadas, pois gestores legitimamente não têm cargo técnico preenchido nessas dimensões.

### 20.2 Bug 2 — `atua_como_gestor` com representação binária inconsistente (`"1"` e `"TRUE"` como valores distintos)

Coluna não fazia parte da lista de 347 binárias detectadas na tipagem da Silver (motivo raiz não investigado a fundo — possivelmente um formato de origem diferente das demais colunas binárias), então não recebeu a conversão padrão para `boolean`. Corrigida pontualmente nesta tabela via `when/otherwise`, tratando `"1"`/`"TRUE"`/`"true"` como `True` e `"0"`/`"FALSE"`/`"false"` como `False`.

### 20.3 Bug 3 — `DISTINCT` nas colunas de contexto não equivale a contagem de respondentes únicos

Ao validar o percentual de "IA é prioridade", uma subquery com `SELECT DISTINCT ano, cargo, nivel, resposta` foi usada para tentar desduplicar as múltiplas linhas geradas pelo unpivot. Isso está **matematicamente errado**: `DISTINCT` nas dimensões de contexto conta combinações únicas de (cargo × nível × resposta), não pessoas — centenas de respondentes com a mesma combinação colapsam em 1 linha, gerando uma contagem artificialmente baixa (25 "respondentes" em vez de centenas/milhares reais) e um percentual não confiável.

**Correção**: adicionado `respondente_id` via `monotonically_increasing_id()` **antes** do unpivot, propagado a todas as linhas geradas a partir de um mesmo respondente. Validação correta usa `SELECT DISTINCT respondente_id, ...` — resultado após a correção: 896 / 1045 / 652 respondentes reais por ano, números de ordem de grandeza plausível.

**Princípio geral para tabelas em formato longo (pós-unpivot)**: qualquer contagem de "quantidade de pessoas" (não de linhas) exige uma chave de identificação do respondente presente na tabela — sem ela, qualquer `DISTINCT`/`GROUP BY` nas colunas de contexto é uma armadilha de contagem incorreta, mesmo quando a query roda sem erro.

### 20.4 Resultado de negócio validado

Percentual de gestores que reportam IA generativa como prioridade da empresa: 36,2% (2023-2024) → 53,6% (2024-2025) → 60,6% (2025-2026) — tendência de crescimento forte e consistente, diretamente relevante à pergunta #5 do desafio.

## 21. Camada Gold — conclusão

Cinco tabelas Gold concluídas: `gold_perfil_mercado`, `gold_remuneracao_por_perfil`, `gold_diversidade`, `gold_adocao_tecnologias`, `gold_adocao_ia`. Todas particionadas por `ano_pesquisa`, catalogadas no Glue Data Catalog, e validadas via Athena com resultados de negócio plausíveis.

## 22. Fechamento do projeto

Todos os entregáveis obrigatórios do Tech Challenge Fase 3 foram concluídos:

| Entregável | Local |
|---|---|
| Pipeline de dados completo (Bronze → Silver → Gold) em AWS | Documentado neste README, seções 1-21 |
| Diagrama de arquitetura da solução | `docs/arquitetura_tech_challenge.drawio` (editável) e `.png` (exportado) |
| Material executivo com DataViz e Storytelling | `entregaveis/panorama_mercado_dados.pptx` (19 slides) |
| Relatório executivo (documento escrito) | `entregaveis/relatorio_executivo_state_of_data.docx` |
| Notebook consolidado com os scripts do pipeline | `notebooks/` (pendente de publicação) |
| Vídeo da apresentação executiva | [link a adicionar] |

Repositório: [github.com/gabrielsbn/state-of-data-brasil-pipeline](https://github.com/gabrielsbn/state-of-data-brasil-pipeline)

### Resumo dos principais achados de negócio

- Analista de Dados segue como o cargo mais comum do mercado (~24%), com o mercado amadurecendo em senioridade (quase metade já é Sênior/Especialista em 2025-2026).
- Engenharia de ML e Arquitetura de Dados lideram remuneração — perfis mais escassos e mais bem pagos.
- Participação feminina em queda consistente nas 3 edições (24,4% → 22,0%).
- SQL, Python e AWS seguem como base técnica dominante; Power BI lidera com folga em BI.
- Priorização de IA generativa entre gestores saltou de 36,2% para 60,6% em 3 anos — a maior tendência identificada no estudo. O principal obstáculo à adoção é técnico (expertise e maturidade de dados), não falta de patrocínio executivo.
- Sudeste concentra 62,3% dos profissionais e paga o maior salário médio — diferencial de até 33% frente ao Nordeste.
