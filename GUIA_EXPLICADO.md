# Guia explicado do trabalho

**Integrantes:** Yan Augusto Scholze e João Victor Mendes.

Leia primeiro este guia para entender o fluxo. Depois ensaie o `ROTEIRO_DEFESA.md`,
que divide as falas e os comandos entre os dois integrantes. Os trechos abaixo
foram extraídos do código desta entrega; as linhas referem-se a essa versão.

## 1. O que o professor pediu

O objetivo é coordenar **três processos independentes**, PeerA, PeerB e PeerC,
para que compartilhem **dois recursos**, R1 e R2, usando Ricart e Agrawala.
Para cada recurso, só um peer pode estar na seção crítica de cada vez.
Peers diferentes podem usar R1 e R2 simultaneamente.

Além disso, deve haver progresso sem impasse/inanição e respeito à ordem causal
dos pedidos. Isso é analisado sob as premissas do enunciado: processos e canal
não falham, e quem entra termina seu uso e libera o recurso.

| Pedido do professor | Onde foi implementado |
| --- | --- |
| Escolher Java RMI ou Pyro5 | Python com Pyro5, `python/requirements.txt` |
| Um programa executado como os três peers | `peer.py`, `main()` |
| Serviço de nomes único, criado pelo primeiro peer | `servico_nomes.py`, `obter_ou_criar_servico_de_nomes()` |
| Esperar e obter a referência dos dois outros peers | `aguardar_peer()` e `main()` |
| Um estado por recurso | `protocolo.py`, `EstadoRecurso`; `Peer.recursos` |
| Relógio de Lamport e desempate por identificador | `solicitar()`, `_recebida()`, `receber_pedido()` |
| Pedidos unicast, OKs e adiamento | `solicitar()`, `receber_pedido()`, `receber_ok()` |
| Duas filas de interesse | `EstadoRecurso.adiados`, uma instância por recurso |
| Política contra impasse entre R1 e R2 | Guardas no início de `solicitar()` |
| Geração e troca de chaves públicas | `gerar_chave()`, `trocar_chaves()`, `receber_chave_publica()` |
| Assinar e verificar pedido, OK e liberação | `assinar()`, `verificar()`, `_mensagem()`, `_validar()` |
| Mostrar uma assinatura inválida sendo rejeitada | `demonstrar_assinatura_invalida()` e opção 9 |
| Publisher/subscriber com callback assíncrono | `receber_pedido()`, `liberar()`, `notificar()` e executor |
| Menu, relógio, estados, filas e log | `menu()`, `mostrar_estado()`, `_registrar()` |
| Comentários, README e integrantes | Docstrings no código, `README.md` e roteiro |

Na avaliação, assinaturas valem **2 pontos** e publisher/subscriber vale **2 pontos**.
O restante cobre algoritmo, dois recursos, concorrência, interface e comentários,
totalizando 10 pontos. O professor exige defesa: **ambos devem saber explicar qualquer
parte**, mesmo que dividam as falas durante a apresentação.
O prazo e a data da defesa ficam na sala virtual/cronograma; o PDF não fixa a data.

## 2. O que está sendo compartilhado

Nesta implementação, R1 e R2 são **recursos lógicos**. O intervalo entre o evento
`ENTROU_SC` e o comando `liberar` representa o uso exclusivo daquele recurso.
SC significa seção crítica: o trecho de execução que exige acesso exclusivo.

Uma analogia: três pessoas compartilham uma impressora R1 e um scanner R2.
Só uma usa a impressora por vez, mas outra pode usar o scanner nesse mesmo período.
A analogia ajuda a entender a exclusão; o programa demonstra esse uso com estados e logs.

Cada processo possui seu próprio relógio e suas próprias estruturas. Eles não
compartilham um dicionário Python em memória: coordenam-se pelas mensagens Pyro5.

O serviço de nomes funciona como uma agenda: transforma `PeerB` na URI que permite
contatar B. Ele não concede recursos nem calcula a ordem dos pedidos.
A decisão de exclusão mútua é feita pelos próprios peers.

## 3. Vocabulário para acompanhar o código

| Termo | Significado neste programa |
| --- | --- |
| `Peer` | Um processo que recebe e envia chamadas remotas |
| `self` | A instância do peer que está executando aquele método |
| `r` | O estado local de um recurso, R1 ou R2 |
| `m` | O conteúdo de uma mensagem recebida/enviada |
| `URI` | Endereço do objeto remoto registrado no Pyro5 |
| `Proxy` | Objeto local usado para chamar métodos do outro processo |
| `@Pyro5.api.expose` | Permite que aquele método seja chamado remotamente |
| `with self.lock` | Protege o estado contra acesso simultâneo por threads locais |
| `set` | Conjunto sem elementos repetidos; usado para os remetentes dos OKs |
| `ThreadPoolExecutor` | Grupo de threads que executa os envios em segundo plano |
| Método com `_` no começo | Convenção de função interna auxiliar |

O lock local e o algoritmo distribuído têm funções distintas: o lock evita que
threads do **mesmo processo** corrompam os dados; Ricart e Agrawala coordena o
acesso ao recurso **entre processos**.

## 4. Inicialização: como os peers se encontram

A entrada do programa é `main()`, no final de `peer.py`.

**Código real:** `python/peer.py`, linhas 428 a 444.

```python
ns = sn.obter_ou_criar_servico_de_nomes(args.nome)
peer = Peer(args.nome, Path(args.logs) / f"{args.nome}.jsonl")
daemon = Pyro5.api.Daemon(host=sn.HOST_NS)
registrado = False
try:
    uri = daemon.register(peer)
    # safe=True evita sobrescrever um peer de outra execução na mesma porta.
    ns.register(args.nome, uri, safe=True)
    registrado = True
    threading.Thread(target=daemon.requestLoop, daemon=True).start()
    print(f"[{args.nome}] registrado no serviço de nomes: {uri}", flush=True)
    for outro in PEERS:
        if outro != args.nome:
            peer.outros_peers[outro] = sn.aguardar_peer(ns, outro, args.nome)
            print(f"[{args.nome}] referência de {outro} obtida.", flush=True)
    peer.trocar_chaves()
    menu(peer)
```

Passo a passo:

1. Obtém ou cria o serviço de nomes em localhost, porta 9090 por padrão.
2. Cria o objeto `Peer`, incluindo um par de chaves RSA novo.
3. Registra esse objeto no daemon Pyro5, que recebe as chamadas de rede.
4. Registra seu nome e sua URI no serviço de nomes.
5. Inicia o atendimento de chamadas numa thread para manter o menu disponível.
6. Espera os outros dois nomes e guarda as URIs resolvidas por lookup.
7. Troca as chaves públicas e só depois abre o menu.

**Código real:** `python/servico_nomes.py`, linhas 60 a 67.

```python
while True:
    try:
        return ns.lookup(nome_outro)  # devolve a URI do objeto remoto
    except Pyro5.errors.NamingError:
        if not avisou:
            print(f"[{meu_nome}] aguardando {nome_outro} entrar...")
            avisou = True
        time.sleep(0.5)
```

`ns.lookup(nome_outro)` consulta a agenda. Se o nome ainda não existe,
a função aguarda e tenta novamente. Isso explica as mensagens de espera quando
você inicia somente PeerA.

As funções do serviço de nomes foram mantidas do código do professor, em um módulo
separado. O primeiro tenta criar; quem perde a disputa pela porta localiza o serviço
que o outro criou. Como o serviço vive dentro de um peer, seu criador deve sair por último.

## 5. O que cada peer guarda

**Código real:** `python/peer.py`, linhas 37 a 45.

```python
self.nome = nome
self.lock = threading.RLock()
self.relogio = 0
self.recursos = {r: EstadoRecurso() for r in RECURSOS}
self.outros_peers = {}  # nome -> URI resolvida pelo serviço de nomes
self.chaves_publicas = {}
self._pems = {}
self._chave_privada = gerar_chave()
self._pem = chave_publica_pem(self._chave_privada)
```

`nome` identifica o processo. `relogio` é seu contador lógico. `recursos` cria uma
instância separada de `EstadoRecurso` para cada recurso. `outros_peers` guarda as
URIs e `chaves_publicas` guarda as chaves dos outros dois processos.

**Código real:** `python/protocolo.py`, linhas 22 a 30.

```python
@dataclass
class EstadoRecurso:
    """Tudo que é específico de R1 ou R2; acessar sob o lock do peer."""

    estado: str = "LIBERADO"
    pedido_timestamp: Optional[int] = None
    pedido_id: Optional[str] = None
    permissoes: set = field(default_factory=set)
    adiados: dict = field(default_factory=dict)
```

- `estado`: LIBERADO, DESEJADO ou OCUPADO.
- `pedido_timestamp`: prioridade temporal do pedido atual daquele peer.
- `pedido_id`: identidade do pedido, como `PeerA:R1:7`, para distinguir rodadas.
- `permissoes`: conjunto de peers que concederam OK para esse pedido.
- `adiados`: pedidos dos outros peers para os quais este peer ainda deve um OK.

`field(default_factory=set/dict)` cria conjuntos/dicionários novos para cada instância.
Assim R1 e R2 não usam a mesma fila nem o mesmo conjunto de permissões.
A fila é representada por um dicionário indexado pelo nome do interessado;
a ordem de exibição/envio usa o par de prioridade.

**Atenção ao significado dos estados:** `LIBERADO` em B quer dizer que B não está
pedindo nem usando aquele recurso. A ainda pode estar usando esse recurso.
`DESEJADO` significa que este peer pediu e espera permissões.
`OCUPADO` significa que este peer recebeu ambas e entrou na seção crítica.

**As filas também são locais:** a fila de C mostra a quem C está devendo resposta.
Ela não é uma fila global que controla o sistema inteiro.

## 6. Ao digitar `pedir R1`

O menu chama `peer.solicitar("R1")`. O método verifica a inicialização e a política
contra impasse; então prepara o pedido:

**Código real:** `python/peer.py`, linhas 259 a 272.

```python
    self.relogio += 1
    r.estado = "DESEJADO"
    r.pedido_timestamp = self.relogio
    r.pedido_id = f"{self.nome}:{recurso}:{self.relogio}"
    r.permissoes.clear()
    mensagens = [self._mensagem("PEDIDO", recurso, destino,
                               r.pedido_timestamp, r.pedido_id,
                               timestamp=r.pedido_timestamp)
                 for destino in sorted(self.outros_peers)]
    self._registrar("PEDIU", recurso=recurso, pedido_id=r.pedido_id,
                    pedido_timestamp=r.pedido_timestamp)
    pedido_id = r.pedido_id
for envelope in mensagens:
    self._workers.submit(self._transmitir, "receber_pedido", envelope)
```

O relógio avança, R1 passa para DESEJADO, o timestamp é congelado como prioridade,
e as permissões da rodada anterior são limpas.
São montadas duas mensagens: uma para B e outra para C.

As mensagens são enviadas individualmente: isso atende ao requisito de **unicast**.
Os dois envios representam o mesmo pedido e levam o mesmo timestamp de prioridade.
O menu retorna enquanto workers fazem as chamadas remotas.

`PEDIU` e `DESEJADO` ainda não significam que o peer pode usar R1.
Ele precisa aguardar o evento `ENTROU_SC`.

## 7. O relógio de Lamport e a ordem dos pedidos

O relógio lógico é um contador inteiro; ele não mede segundos.
Ao criar um pedido ou outra mensagem do protocolo, o programa avança o contador.
Ao receber uma mensagem válida:

**Código real:** `python/peer.py`, linhas 171 a 174.

```python
def _recebida(self, m):
    """Evento de recepção válido: L = max(L, timestamp_remoto) + 1. Sob lock."""
    self.relogio = max(self.relogio, m["timestamp"]) + 1
    self._registrar("RECEBIDA", mensagem=m, assinatura="VALIDA")
```

Exemplo didático: se o relógio local é 4 e a mensagem traz 9, ele passa a 10.
Se o local é 12 e a mensagem traz 9, ele passa a 13.
Esse mecanismo preserva a ordem causal: se um peer recebe um pedido e depois cria
outro, seu novo pedido terá timestamp maior que o recebido.
Timestamps menores, por si só, não provam causalidade entre eventos concorrentes.

Na comparação entre pedidos, usa-se `(pedido_timestamp, nome)`.
Exemplos didáticos:

- `(5, PeerC)` tem prioridade sobre `(8, PeerA)`, pois 5 < 8.
- `(8, PeerA)` tem prioridade sobre `(8, PeerB)`, pois os timestamps empatam e A < B.

A comparação de tuplas do Python faz exatamente isso: compara o primeiro elemento
e, em caso de igualdade, o segundo. O timestamp usado na prioridade é o do pedido
original, não o relógio atual depois de receber outras mensagens.

## 8. O que o destinatário faz ao receber um pedido

`receber_pedido()` primeiro valida a mensagem e sua assinatura. Mensagens válidas
atualizam o relógio; pedidos repetidos são filtrados. A decisão principal é:

**Código real:** `python/peer.py`, linhas 190 a 205.

```python
    r = self.recursos[m["recurso"]]
    prioridade_remota = (m["pedido_timestamp"], m["remetente"])
    prioridade_local = (r.pedido_timestamp, self.nome)
    adiar = (r.estado == "OCUPADO" or
             (r.estado == "DESEJADO" and prioridade_local < prioridade_remota))
    if adiar:
        # Esta fila É o registro de interesse subscriber -> publisher.
        r.adiados[m["remetente"]] = m
        self._registrar("INTERESSE_REGISTRADO", recurso=m["recurso"],
                        subscriber=m["remetente"], pedido_id=m["pedido_id"],
                        pedido_timestamp=m["pedido_timestamp"])
    else:
        resposta = self._mensagem("OK", m["recurso"], m["remetente"],
                                 m["pedido_timestamp"], m["pedido_id"])
if resposta:
    self._workers.submit(self._transmitir, "receber_ok", resposta)
```

Há três situações:

| Estado local do recurso | Resposta |
| --- | --- |
| LIBERADO | OK imediato |
| OCUPADO | Adiar até sair da seção crítica |
| DESEJADO | Adiar se o pedido próprio tiver menor par; caso contrário enviar OK |

Quando adia, `r.adiados[remetente] = m` registra o interesse do remetente.
Quando concede, gera uma mensagem assinada do tipo OK e agenda `receber_ok` no outro peer.

O `return True` de `receber_pedido()` apenas confirma que a mensagem foi aceita
pelo handler. **A permissão do algoritmo é a mensagem assinada OK/LIBERACAO**,
processada por `_receber_permissao()`. O booleano retornado pela chamada de rede
não entra no conjunto de permissões.

## 9. Como o peer sabe que já pode entrar

`receber_ok()` e `notificar()` chamam o mesmo método auxiliar,
`_receber_permissao()`. Ele verifica a assinatura, o pedido atual e a duplicidade.
Depois executa:

**Código real:** `python/peer.py`, linhas 223 a 231.

```python
r.permissoes.add(m["remetente"])
self._registrar("PERMISSAO_RECEBIDA", recurso=m["recurso"],
                remetente=m["remetente"], tipo=tipo,
                quantidade=len(r.permissoes), pedido_id=r.pedido_id)
if r.permissoes == set(PEERS) - {self.nome}:
    r.estado = "OCUPADO"
    self._registrar("ENTROU_SC", recurso=m["recurso"],
                    pedido_id=r.pedido_id, pedido_timestamp=r.pedido_timestamp,
                    permissoes=sorted(r.permissoes))
```

Para PeerA, `set(PEERS) - {self.nome}` é `{PeerB, PeerC}`.
Portanto, são necessárias as duas permissões de nomes distintos.
Um conjunto evita que dois OKs de B sejam contados como se fossem de B e C.
O `pedido_id` evita que uma permissão antiga autorize um pedido novo.

Exemplo: A recebeu OK de B, mas C ainda usa R1. A permanece DESEJADO.
Quando C liberar e enviar a notificação válida, A completa o conjunto e fica OCUPADO.

## 10. Liberação e publisher/subscriber

Na parte protegida por lock de `liberar()`, o programa:

1. Confere que o recurso está OCUPADO.
2. Copia os interessados e marca o recurso como LIBERADO.
3. Limpa o pedido atual, as permissões e a fila daquele recurso.
4. Cria uma mensagem LIBERACAO assinada para cada interessado.

Depois de sair do lock:

**Código real:** `python/peer.py`, linhas 304 a 305.

```python
for envelope in mensagens:
    self._workers.submit(self._transmitir, "notificar", envelope)
```

`submit` agenda o envio em uma thread do executor; a thread do menu não espera
o destinatário processar a notificação. O método de rede chamado é `notificar()`.
A chamada de rede dentro do worker é síncrona; o envio é assíncrono em relação
à operação local de liberação porque ocorre em outra thread.

| Papel | Onde aparece |
| --- | --- |
| Subscriber | Peer que pediu e ficou esperando uma resposta |
| Registro de interesse | Pedido guardado em `r.adiados` do outro peer |
| Publisher | Peer que reteve o OK e depois libera o recurso |
| Evento | Liberação do recurso, representada pelo OK antes adiado |
| Callback | Publisher chama `notificar()` no subscriber |

Um mesmo peer exerce os dois papéis em situações diferentes.
Não é preciso um broker central nem um método separado de inscrição:
o pedido adiado é a inscrição exigida pelo professor.

**A notificação é uma permissão entre as duas necessárias.** Se falta o OK de
um terceiro peer, o subscriber continua esperando.
R1 e R2 têm filas diferentes; liberar R1 não esvazia a fila de R2.

## 11. Assinatura digital: o que é assinado

Todo peer gera um par de chaves RSA. A privada assina e fica em memória;
a pública é enviada aos outros para que verifiquem suas mensagens.
`trocar_chaves()` envia a pública e `receber_chave_publica()` a cadastra.
A referência remota e a chave pública são duas informações distintas sobre o mesmo peer.

**Código real:** `python/protocolo.py`, linhas 59 a 63.

```python
def assinar(privada, conteudo):
    """Assina todos os campos; PKCS1v15/SHA256 equivale a SHA256withRSA."""
    assinatura = privada.sign(serializar(conteudo), padding.PKCS1v15(), hashes.SHA256())
    return {"conteudo": dict(conteudo),
            "assinatura": base64.b64encode(assinatura).decode("ascii")}
```

Antes de assinar, `serializar()` converte o conteúdo em JSON canônico: mesma ordem
de chaves e mesma representação dos bytes em todos os peers.
`SHA256` é a função de hash e `PKCS1v15` é o preenchimento usado com RSA,
correspondendo a SHA256withRSA.
Base64 apenas representa os bytes da assinatura como texto para o Pyro5.

São assinados remetente, destinatário, tipo, recurso, timestamp,
`pedido_timestamp` e `pedido_id`. Os tipos são PEDIDO, OK e LIBERACAO.
Em um OK/notificação, o timestamp de envio é novo, mas os campos do pedido
continuam identificando a rodada para a qual a permissão foi concedida.

**Código real:** `python/protocolo.py`, linhas 66 a 70.

```python
def verificar(chave, envelope):
    """Lança InvalidSignature/ValueError se bytes ou conteúdo foram adulterados."""
    assinatura = base64.b64decode(envelope["assinatura"], validate=True)
    chave.verify(assinatura, serializar(envelope["conteudo"]),
                 padding.PKCS1v15(), hashes.SHA256())
```

Se o conteúdo foi alterado depois da assinatura, `verify()` falha.
`Peer._validar()` intercepta a falha, registra `MENSAGEM_REJEITADA` e retorna `None`.
O handler termina antes de atualizar relógio, filas, estados e permissões.
O log de diagnóstico é atualizado para registrar a rejeição.

A assinatura permite verificar integridade e posse da chave privada correspondente
à pública cadastrada. Não cifra o conteúdo. O bootstrap usa os nomes conhecidos
sob as premissas do exercício; não há uma autoridade certificadora.

### Como a demonstração de mensagem adulterada funciona

**Código real:** `python/peer.py`, linhas 319 a 325.

```python
    # Mantém o formato coerente; SOMENTE a assinatura fica inválida.
    m = envelope["conteudo"]
    m["timestamp"] += 1
    m["pedido_timestamp"] += 1
    m["pedido_id"] = f"{self.nome}:{recurso}:{ts + 1}"
    self._registrar("ADULTERACAO_DEMONSTRACAO", destinatario=destino)
self._workers.submit(self._transmitir, "receber_pedido", envelope)
```

A assinatura foi criada antes desse trecho. O código altera timestamp e campos
associados sem gerar uma nova assinatura. O formato continua coerente,
mas a assinatura deixa de corresponder aos bytes recebidos.
Use `adulterar PeerB R1` em A e observe a rejeição em B.

## 12. Por que há locks e threads

Menu, atendimento de chamadas remotas e envio de mensagens podem rodar ao mesmo
tempo. `RLock` protege as estruturas locais. Ele é reentrante porque métodos que
já estão sob o lock chamam auxiliares, como `_registrar()`, que também o adquirem.

O padrão importante é: preparar/alterar estado **sob lock**, soltar o lock e então
fazer a chamada remota. Em `receber_pedido()`, o `submit` fica fora do bloco `with`.
Em `liberar()`, os envios também ficam depois do bloco.

**Código real:** `python/peer.py`, linhas 143 a 144.

```python
with Pyro5.api.Proxy(self.outros_peers[destino]) as outro:
    aceita = getattr(outro, metodo)(envelope)
```

O Proxy é criado no worker que o utiliza. Isso evita a restrição do Pyro5 de um
Proxy pertencer à thread que o criou. Segurar o lock local enquanto se espera
uma chamada remota pode formar uma dependência circular entre A e B.

## 13. A política contra impasse entre os dois recursos

O problema que o professor pediu para tratar é:
A mantém R1 e pede R2; B mantém R2 e pede R1. Nenhum libera o que possui.

A política implementada é adquirir em ordem **R1 → R2**, com duas verificações:

**Código real:** `python/peer.py`, linhas 254 a 258.

```python
# Ordem global: nunca adquirir R1 enquanto pede ou mantém R2.
if recurso == "R1" and self.recursos["R2"].estado != "LIBERADO":
    raise ValueError("política anti-impasse R1 -> R2: libere R2 antes de pedir R1")
if recurso == "R2" and self.recursos["R1"].estado == "DESEJADO":
    raise ValueError("política anti-impasse R1 -> R2: espere R1 ficar OCUPADO antes de pedir R2")
```

A primeira proíbe pedir R1 quando R2 está pendente ou ocupado.
A segunda proíbe pedir R2 enquanto ainda se espera por R1. Sem a segunda,
R2 poderia ser concedido primeiro e o peer terminaria segurando o recurso maior
enquanto espera o menor, recriando a possibilidade de ciclo.

É permitido usar R2 sozinho: nesse caso, enquanto o usa/espera, não pode pedir R1.
É permitido manter os dois: primeiro obtém R1, depois pede R2.
É permitido que A mantenha R1 enquanto B mantém R2; são recursos independentes.

## 14. Logs, menu e arquivos

`_registrar()` escreve eventos na tela e num arquivo JSONL por peer. Cada linha
é um objeto JSON com evento, relógio e dados relevantes.
`PEDIU` registra a intenção; `RECEBIDA` registra a mensagem com assinatura válida;
`INTERESSE_REGISTRADO` mostra o adiamento; `ENTROU_SC` mostra o acesso;
`SAIU_SC` mostra a liberação; `MENSAGEM_REJEITADA` mostra a verificação inválida.

O campo de horário real nos logs é de diagnóstico; a prioridade do algoritmo usa
Lamport. Não confunda `instante_ns` com `pedido_timestamp`.

`consultar_estado()` devolve uma cópia do estado para consulta. `mostrar_estado()`
a apresenta no terminal. `menu()` traduz números ou comandos textuais em chamadas
locais. Pedir/liberar são comandos locais; os outros peers chamam os handlers expostos.

`protocolo.py` agrupa dados e criptografia. `servico_nomes.py` agrupa descoberta.
`peer.py` reúne algoritmo, interface remota e interface de terminal.
Os scripts `preparar.sh` e `executar.sh` facilitam instalar e iniciar.

## 15. O que os testes demonstraram

O resultado salvo em `validacao/resultado_testes.txt` tem 20 testes aprovados:
13 determinísticos e 7 cenários de integração. Os determinísticos controlam a entrega
para provocar exatamente um empate ou uma duplicata, mas usam RSA real.
Os de integração iniciam três processos e usam chamadas Pyro5 reais.

Há verificação de dois recursos, ordenação, assinaturas, uma notificação insuficiente,
OK antigo, callback assíncrono, barreira de inicialização e início simultâneo.
As sessões reais registraram 27 entradas e 27 saídas de SC, sem sobreposição observada
por recurso. São resultados de execuções finitas; a explicação do algoritmo e das
premissas continua necessária na defesa.

## 16. Exercício para ambos antes da defesa

Sem ler, cada integrante deve conseguir responder:

1. Em que momento um pedido passa de DESEJADO para OCUPADO?
2. Por que retornar True de `receber_pedido` não equivale a enviar OK?
3. O que faz A adiar o pedido de B?
4. O que acontece se chegarem dois OKs de B?
5. Quem é publisher e quem é subscriber num pedido adiado?
6. O que muda quando chega uma assinatura inválida?
7. Por que R2 é recusado enquanto R1 está DESEJADO?
8. Por que o lock local não substitui Ricart e Agrawala?
9. Por que o serviço de nomes não é um coordenador da SC?
10. Por que o criador do serviço de nomes deve ser encerrado por último?

As respostas estão nos tópicos acima. Para ensaiar falas, transições e operação
dos três terminais, siga o `ROTEIRO_DEFESA.md`.
