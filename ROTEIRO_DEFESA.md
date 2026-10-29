# Roteiro da defesa — Yan Augusto Scholze e João Victor Mendes

**Preparação:** leiam primeiro `GUIA_EXPLICADO.md`. Este roteiro sugere falas e
operação dos terminais para uma apresentação de aproximadamente 12 a 15 minutos;
ajustem ao tempo que o professor permitir. A divisão organiza a apresentação:
**os dois precisam saber responder sobre qualquer parte do código**, como exige o PDF.

Mantenham três terminais identificados como PeerA, PeerB e PeerC, além dos arquivos
`python/peer.py`, `python/protocolo.py` e `python/servico_nomes.py` abertos no editor.
Quem estiver falando aponta o código; o outro executa os comandos indicados.

| Etapa | Quem explica | Quem opera | O que mostrar |
| --- | --- | --- | --- |
| 1. Objetivo | Yan | João prepara os terminais | Três peers, dois recursos e requisitos |
| 2. Inicialização | João | Yan inicia os peers | Serviço de nomes, lookup e chaves |
| 3. Algoritmo | Yan | João faz acesso sem concorrência | Estado, Lamport, pedido e dois OKs |
| 4. Assinaturas | João | Yan envia mensagem adulterada | Assinar, verificar e rejeitar |
| 5. Concorrência | Yan | João cria os pedidos | Filas e prioridade por timestamp/nome |
| 6. Notificação | João | Yan libera os recursos | Callback assíncrono e contagem de permissões |
| 7. Dois recursos | Yan | João demonstra | Independência e política contra impasse |
| 8. Validação | João | Yan mostra os resultados | Testes e encerramento correto |

## Etapa 1 — Yan apresenta o problema

**Fala sugerida:**

> O trabalho pede três processos pares que disputam dois recursos. Cada processo
> pode pedir acesso, conceder permissão e notificar os outros. Escolhemos Python
> com Pyro5. Para cada recurso, só um peer pode estar na seção crítica por vez;
> recursos diferentes podem ser usados ao mesmo tempo. Usamos Ricart e Agrawala
> com relógio de Lamport para ordenar pedidos, assinaturas para conferir as
> mensagens e callbacks para entregar permissões que foram adiadas.

**Apontar:** `protocolo.py`, linhas 15 a 30: nomes, recursos e `EstadoRecurso`.
Explique que a seção crítica é o intervalo em que o estado local fica OCUPADO.

**Transição:** “O João vai mostrar como os três processos se encontram.”

## Etapa 2 — João explica a inicialização

**Yan executa**, na pasta `python`, uma linha em cada terminal:

```bash
bash executar.sh PeerA
bash executar.sh PeerB
bash executar.sh PeerC
```

As três linhas representam três terminais, não uma sequência que avança num só.
Se o ambiente ainda não foi preparado, execute `bash preparar.sh` uma vez antes.

**Fala sugerida:**

> O primeiro peer cria o serviço de nomes; os outros usam o mesmo serviço.
> Cada peer registra seu nome e espera os outros dois para obter suas URIs por
> lookup. O serviço é uma agenda de objetos remotos. Quem concede os recursos
> são os peers, e não essa agenda. Depois trocamos as chaves públicas. O menu
> só fica disponível quando este peer possui as duas referências, as duas chaves
> e já enviou sua própria chave pública.

**Apontar:** `servico_nomes.py:15`, `servico_nomes.py:57`, `peer.py:418`,
`peer.py:103` e `peer.py:116`.
Mostre na tela `CHAVE_RECEBIDA`, `CHAVE_ENVIADA` e `PRONTO`.
Explique que `@Pyro5.api.expose` permite chamar um método a partir de outro processo.

**Transição:** “Com os peers prontos, o Yan mostra como um pedido vira acesso.”

## Etapa 3 — Yan explica o algoritmo e o acesso simples

**João executa:** PeerA `pedir R1`; aguarda `ENTROU_SC`; digita `estado`.

**Fala sugerida:**

> Ao pedir R1, incrementamos o relógio de Lamport, marcamos DESEJADO e enviamos
> dois pedidos unicast, um para cada outro peer. Quem recebe valida a assinatura
> e atualiza seu relógio. Se estiver usando o recurso, adia o OK. Se também
> estiver pedindo, compara o par timestamp e nome. O menor par tem prioridade.
> Como aqui não há concorrência, chegam os dois OKs e A passa a OCUPADO.

**Apontar, nesta ordem:**

1. `peer.py:244`: `solicitar`, preparação e envio do pedido.
2. `peer.py:171`: atualização `max(local, remoto) + 1`.
3. `peer.py:190`: comparação dos pares e decisão de adiar.
4. `peer.py:208`: validação da permissão atual e filtro de duplicatas.
5. `peer.py:227`: comparação com os dois outros nomes para entrar.

**Exemplo verbal, com números didáticos:** “Um pedido `(5, PeerC)` precede
`(8, PeerA)`. Se ambos têm timestamp 8, PeerA precede PeerB.”

**João executa:** PeerA `liberar R1`; mostra o estado LIBERADO.

**Frase importante:** “Usamos um conjunto de nomes; receber duas mensagens de B
não conta como receber de B e C. O identificador do pedido também impede usar um OK antigo.”

**Transição:** “Agora o João mostra o que impede uma mensagem alterada de afetar esse estado.”

## Etapa 4 — João explica assinaturas e demonstra a rejeição

**Apontar:** `protocolo.py:33`, `protocolo.py:39`, `protocolo.py:59`,
`protocolo.py:66`, `peer.py:151` e `peer.py:307`.

**Fala sugerida:**

> Cada peer gera um par RSA de 2048 bits. Ele compartilha a pública e guarda a
> privada. Assinamos o conteúdo completo da mensagem com SHA-256 e RSA. O JSON
> canônico garante que remetente e destinatário obtenham os mesmos bytes.
> No recebimento, verificamos a assinatura com a pública cadastrada. A verificação
> acontece antes de mudar relógio, estados, filas ou permissões. A assinatura
> detecta alteração e verifica a posse da chave correspondente; o conteúdo continua legível.

**Yan executa:**

1. Em B: `estado` para registrar a situação inicial.
2. Em A: `adulterar PeerB R1`.
3. Em B: mostra `MENSAGEM_REJEITADA assinatura=INVALIDA` e consulta `estado` novamente.

**João acrescenta:** “Essa opção assina e depois muda o conteúdo, mantendo o formato
coerente. A mensagem é rejeitada pela assinatura. O log registra a tentativa, mas
os dados do algoritmo não são alterados por ela.”

**Transição:** “Vamos criar uma disputa real para enxergar filas, ordem e notificações.”

## Etapa 5 — Yan explica a disputa com os três peers

**João executa, respeitando as esperas:**

1. C: `pedir R1`; espera C ficar OCUPADO.
2. B: `pedir R1`.
3. Espera o log de A mostrar o pedido válido de B; em C, `estado` deve mostrar B na fila.
4. A: `pedir R1`.
5. C: `estado`; mostra os dois registros de interesse e seus timestamps.

**Fala sugerida:**

> C está usando R1, então retém seus OKs. Os pedidos de B e A ficam registrados
> na fila de C. Como A recebeu o pedido de B antes de criar o seu, Lamport faz
> o pedido de A ter timestamp maior. Por isso B entra primeiro nesta sequência.
> O critério é o par timestamp e nome; em pedidos concorrentes, não usamos
> o horário do computador nem assumimos a ordem pelo momento do clique.

**Apontar:** `peer.py:190` para prioridade; `peer.py:197` para o registro de interesse.

**Transição:** “O João explica como C responde aos interessados quando libera.”

## Etapa 6 — João explica publisher/subscriber e o callback assíncrono

**Yan executa:** C `liberar R1`; aguarda B ficar OCUPADO e consulta o estado de A.

**Fala sugerida:**

> Quando B e A tiveram os pedidos adiados por C, eles se tornaram subscribers
> da liberação de R1 em C. C é o publisher: ao liberar, retira os interesses da
> fila e agenda um callback notificar para cada interessado. O executor faz os
> envios em outras threads; a liberação não fica esperando os destinatários.
> Essa notificação é o OK que tinha sido adiado. A ainda não entra só com o OK
> de C: também precisa do de B.

**Apontar:** `peer.py:275` para a liberação, `peer.py:304` para `submit`,
`peer.py:239` para `notificar` e `peer.py:208` para contar permissões.

**Yan executa:** B `liberar R1`; aguarda A ficar OCUPADO; A `liberar R1`.
Mostra filas vazias e `INTERESSE_REMOVIDO`.

**João acrescenta:** “As chamadas de rede ficam fora do lock local. Cada worker
cria seu próprio Proxy, seguindo a regra do Pyro5 sobre threads.”
**Apontar:** `peer.py:143` e o `submit` fora do bloco `with` em `liberar()`.

**Transição:** “O Yan mostra como os dois recursos continuam independentes e como evitamos impasse.”

## Etapa 7 — Yan explica recursos independentes e a ordem contra impasse

**João executa:**

1. A: `pedir R1`; espera OCUPADO.
2. B: `pedir R2`; espera OCUPADO.
3. Mostra `estado` de A e B: recursos diferentes estão em uso ao mesmo tempo.
4. B: `pedir R1`; mostra a recusa por ordem de aquisição.
5. B: `liberar R2`.
6. A: `pedir R2`; aguarda OCUPADO nos dois recursos.
7. A: `liberar R2`; depois `liberar R1`.

**Fala sugerida:**

> Cada recurso tem seu estado e sua fila; por isso A pode usar R1 enquanto B
> usa R2. Para evitar espera circular, adotamos aquisição R1 e depois R2.
> Quem já pede ou mantém R2 não pode pedir R1. Também impedimos pedir R2
> enquanto R1 ainda está DESEJADO: é preciso obtê-lo primeiro. Sem esse cuidado,
> R2 poderia ser concedido antes e recriar o ciclo entre os recursos.

**Apontar:** `peer.py:254` e as duas verificações dentro do lock.

**Transição:** “O João mostra os testes e encerra a execução.”

## Etapa 8 — João apresenta a validação

**Fala sugerida:**

> Temos 20 testes aprovados no resultado salvo: 13 casos determinísticos e
> 7 cenários de integração. Os determinísticos forçam situações como empate,
> duplicata e uma única notificação insuficiente. Os de integração iniciam três
> processos e fazem chamadas Pyro5 reais. Eles verificam concorrência, dois
> recursos, assinatura adulterada, progresso dos pedidos e início simultâneo.
> A correção é explicada junto às premissas de ausência de falhas e liberação eventual.

**Yan mostra:** `validacao/resultado_testes.txt` e, se solicitado,
`python/tests/test_protocolo.py:86` para o empate,
`python/tests/test_protocolo.py:119` para notificação insuficiente e
`python/tests/test_integracao.py:168` para a disputa entre processos.

**Encerramento:** com todos os recursos LIBERADO, Yan digita `sair` em B e C;
por último em A, que foi iniciado primeiro e criou o serviço de nomes.

---

# Consulta técnica para ensaio e perguntas

A seguir ficam os passos detalhados e as respostas de apoio.


Comece com os três peers e aguarde `PRONTO` em todos. Cada comando abaixo é digitado
no terminal do peer indicado. Para cada demonstração, os recursos devem começar
`LIBERADO`; consulte `estado` nos três antes de prosseguir.

## 1. Inicialização e comunicação

Inicie PeerA, depois PeerB e PeerC. Explique que PeerA cria o serviço de nomes e os
outros obtêm sua referência. Enquanto faltam peers, as mensagens de espera são esperadas.
Mostre `nomes`, `ola` e os registros `CHAVE_ENVIADA`, `CHAVE_RECEBIDA`, `PRONTO`.
O serviço de nomes só localiza os objetos, não escolhe quem entra na seção crítica.

## 2. Acesso sem concorrência

1. PeerA: `pedir R1`.
2. Aguarde `ENTROU_SC recurso=R1` e consulte `estado`: R1 está `OCUPADO`, com dois OKs.
3. PeerA: `liberar R1`. O estado volta a `LIBERADO`.

## 3. Dois pedidos com o terceiro peer na seção crítica

1. PeerC: `pedir R1`; aguarde `OCUPADO`.
2. PeerB: `pedir R1`. Espere aparecer o pedido de B no log do PeerA e na fila do PeerC.
3. PeerA: `pedir R1`.
4. Consulte `estado` nos três. PeerC está `OCUPADO`; A e B estão `DESEJADO`.
   A fila de C mostra ambos os pedidos com timestamps.
5. PeerC: `liberar R1`.
6. O peer com menor `(pedido_timestamp, nome)` entra primeiro. Na sequência acima,
   B pediu e seu pedido foi recebido por A antes de A pedir; portanto B tem prioridade.
7. Consulte `estado`: A ainda espera. B: `liberar R1`; agora A entra.
8. A: `liberar R1`. Mostre as filas vazias e os logs `INTERESSE_REMOVIDO`.

Explique: C notifica os dois sem esperar seus handlers. Essas notificações são OKs
adiados. A não entra só por receber o de C; também precisa do de B.
A ordenação é lógica, não o horário do relógio do computador.
Se os timestamps empatarem, o nome resolve: PeerA antes de PeerB antes de PeerC.
O teste determinístico força exatamente esse empate.

## 4. Dois recursos independentes

1. PeerA: `pedir R1`; aguarde `OCUPADO`.
2. PeerB: `pedir R2`; aguarde `OCUPADO`.
3. Mostre que A usa R1 e B usa R2 simultaneamente, com filas distintas.
4. PeerB: `pedir R1`. Deve ser recusado pela política R1 → R2.
5. PeerB: `liberar R2`.
6. PeerA: `pedir R2`; agora A pode ficar `OCUPADO` nos dois recursos.
7. PeerA: `liberar R2` e depois `liberar R1`.

Explique: aceitar B segurando R2 e pedindo R1 permitiria formar um ciclo caso A
segurasse R1 e pedisse R2. A verificação do menu também existe no método `solicitar`,
no mesmo lock que atualiza o estado, portanto a regra está implementada no algoritmo.

## 5. Assinatura inválida

1. PeerB: `estado`; anote relógio/estados/filas.
2. PeerA: `adulterar PeerB R1` (ou use a opção 9, que escolhe o primeiro outro peer).
3. PeerB mostra `MENSAGEM_REJEITADA assinatura=INVALIDA`.
4. PeerB: `estado`; relógio/estados/filas não mudaram por causa da mensagem inválida.
5. PeerA: `pedir R1`; mostre que o pedido válido ainda funciona e depois libere.

A alteração feita depois de assinar mantém o formato do pedido válido. A rejeição
ocorre na verificação criptográfica, não apenas por erro de formato.

## 6. O que cada método faz

| Arquivo/método | Explicação para a defesa |
| --- | --- |
| `servico_nomes.obter_ou_criar_servico_de_nomes` | Localiza ou cria o serviço; trata a disputa pela porta |
| `servico_nomes.aguardar_peer` | Espera o registro e devolve a URI obtida por lookup |
| `Peer.trocar_chaves` | Envia a chave pública e espera conhecer ambas as outras |
| `Peer.solicitar` | Marca DESEJADO, define prioridade e envia os dois pedidos unicast |
| `Peer.receber_pedido` | Compara estado/prioridade; responde OK ou registra interesse |
| `Peer.receber_ok` | Confere assinatura e contabiliza permissão imediata |
| `Peer.liberar` | Sai da SC, limpa interesses e agenda notificações em workers |
| `Peer.notificar` | Recebe o OK adiado, validando e contando como uma única permissão |
| `Peer._receber_permissao` | Filtra duplicatas/pedidos antigos e só entra com os dois peers |
| `protocolo.assinar/verificar` | Assina/verifica os mesmos bytes do JSON canônico |

## 7. Perguntas prováveis

**Por que não usar apenas um contador de OKs?** Um conjunto de nomes impede que
uma mensagem repetida conte duas vezes. Também confere o identificador do pedido.

**Por que Lamport?** A recepção usa `max(local, remoto)+1`; isso preserva a ordem
causal. O par com o nome produz ordem total para pedidos concorrentes de mesmo timestamp.

**Por que há dois timestamps em um OK?** Um é o envio atual da mensagem; o outro
identifica o pedido ao qual a permissão pertence. No PEDIDO, ambos são iguais.

**A fila é um broker central?** Não. Cada peer mantém sua própria fila para cada recurso.
O pedido adiado inscreve o outro peer naquele publisher; a liberação dispara o callback.

**A notificação bloqueia quem libera?** O método `liberar` apenas agenda o trabalho.
`notificar` é chamado numa thread do executor, com Proxy criado nessa thread.

**Por que soltar o lock antes de uma chamada remota?** Segurar o lock enquanto A
chama B e B tenta chamar A pode travar o acesso ao estado local dos dois.

**O que garante segurança?** Para dois pedidos ao mesmo recurso, o peer ocupado
retém sua permissão, ou o par de prioridade menor retém o OK do maior. Ambos não
conseguem juntar todas as permissões ao mesmo tempo.

**E progresso?** Sem falhas e com liberação eventual da SC, o menor pedido pendente
obtém os OKs e entra; ao sair, concede os adiados. Pedidos futuros recebem timestamps
maiores no peer que recebeu esse pedido. A ordem R1 → R2 impede ciclos entre recursos;
R2 também é recusado enquanto o peer ainda espera R1, para não ganhar R2 primeiro.

**A assinatura cifra a mensagem?** Não. Ela autentica a posse da chave privada e
permite detectar alteração do conteúdo. Apenas a chave pública é compartilhada.

**E se um peer falhar?** O enunciado exclui falhas de processo e canal. A aplicação
registra erros; não finge receber OK por timeout e não implementa recuperação.

## 8. Encerrar e entregar

Com pedidos concluídos, libere os recursos e encerre os dois peers que não criaram
o serviço; encerre o criador por último. Envie o ZIP com o código e o README na sala
virtual. A data é a informada na atividade/cronograma; o PDF não fixa uma data.
Ambos os integrantes devem estar presentes e saber explicar qualquer parte.
