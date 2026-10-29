# Trabalho 1 — Exclusão mútua distribuída P2P

**Integrantes:** Yan Augusto Scholze e João Victor Mendes  
**Disciplina:** Sistemas Distribuídos — Engenharia de Computação — IFC São Bento do Sul  
**Professor:** Lucas Dominguez Cordeiro  
**Linguagem/middleware escolhido:** Python 3.10+ com Pyro5  
**Recursos:** R1 e R2, representados por seções críticas mantidas até a liberação pelo menu.

Implementação do algoritmo de Ricart e Agrawala para três processos na mesma máquina,
com relógio de Lamport, duas filas, RSA/SHA-256 e publisher/subscriber assíncrono.
Um único programa é executado como `PeerA`, `PeerB` e `PeerC`. Cada peer pede,
concede e notifica; não existe coordenador central de permissões.
O serviço de nomes apenas registra nomes e URIs.

## Preparar e executar

Não há compilação de Java: a entrega escolhe a alternativa **Python/Pyro5** permitida
pelo enunciado. A pasta `java/` da cópia de trabalho é o esqueleto original do professor;
ela não faz parte do ZIP da solução.

No Linux, a partir desta pasta, execute uma vez:

```bash
cd python
bash preparar.sh
```

Abra **três terminais**, entre na pasta `python` em cada um e execute:

```bash
# Terminal 1
bash executar.sh PeerA
```

```bash
# Terminal 2
bash executar.sh PeerB
```

```bash
# Terminal 3
bash executar.sh PeerC
```

O script usa `.venv/bin/python`; não é necessário ativar o ambiente manualmente.
A primeira execução pode aguardar os outros dois peers, o que é esperado.
O menu só aparece após o lookup das duas referências e a troca das duas chaves públicas.
Não execute `pyro5-ns` à parte.

Alternativa manual, incluindo Windows:

```bash
python -m venv .venv
# Linux: source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python peer.py PeerA
# Em outros terminais, com o mesmo ambiente: PeerB e PeerC.
```

No Linux, caso `python` não exista fora do ambiente, use `python3` para criá-lo.
A instalação exige acesso à internet para obter Pyro5, cryptography e suas dependências.
Depois da instalação a aplicação comunica-se apenas na máquina local.

## Menu

| Opção | Comando equivalente | Efeito |
| --- | --- | --- |
| 1 | `pedir R1` | Inicia pedido de R1 sem bloquear o menu |
| 2 | `liberar R1` | Sai da seção crítica de R1 e agenda callbacks |
| 3 | `pedir R2` | Inicia pedido de R2 |
| 4 | `liberar R2` | Sai da seção crítica de R2 e agenda callbacks |
| 5 | `estado` | Exibe estados, relógio, timestamp, permissões e as duas filas |
| 6 | `log` | Exibe os últimos 30 eventos JSON |
| 7 | `ola` | Diagnóstico unicast do esqueleto |
| 8 | `nomes` | Lista registros do serviço de nomes |
| 9 | `adulterar` | Envia um pedido com assinatura inválida a um dos outros peers |
| 0 | `sair` | Encerra se não houver recurso/pedido/interesse pendente |

Também é possível escolher o destinatário da demonstração:
`adulterar PeerB R1`. O comando assina um conteúdo e altera o timestamp e a
identidade correspondente depois da assinatura, mantendo o formato válido.
O destinatário registra `MENSAGEM_REJEITADA assinatura=INVALIDA` e descarta a mensagem
antes de alterar relógio, estados, permissões ou filas.

`DESEJADO` significa esperando as duas permissões. Só o evento `ENTROU_SC`, com estado
`OCUPADO`, permite considerar o recurso em uso. A liberação é manual pelo menu.
O menu permanece disponível durante a espera. Pedidos repetidos de um recurso já
`DESEJADO`/`OCUPADO` e liberação de um recurso não ocupado são recusados.

## Política implementada contra impasse: ordem global R1 → R2

Um peer pode pedir R1 sozinho, R2 sozinho ou os dois na ordem R1 → R2.
Para usar ambos, peça R1, aguarde `OCUPADO` e peça R2.
Se R1 estiver `DESEJADO`, o pedido de R2 é recusado até R1 ficar `OCUPADO`:
ordenar apenas o envio dos pedidos não bastaria, pois R2 poderia ser concedido primeiro.
Se R2 estiver `DESEJADO` ou `OCUPADO`, o código recusa um novo pedido de R1,
inclusive se o usuário tentar usar o menu fora da ordem.

Assim, nenhum processo pode manter R2 e esperar por R1; elimina-se o ciclo
A mantém R1 e espera R2 / B mantém R2 e espera R1. A regra é verificada sob o mesmo
lock que muda os estados. Peers diferentes continuam podendo usar R1 e R2 ao mesmo
tempo. Recomenda-se liberar em ordem inversa, R2 e depois R1; essa ordem de liberação
não é necessária para a ausência de espera circular.

## Algoritmo e mensagens

Cada peer mantém **um relógio lógico** e **dois estados independentes**. Para cada
recurso há timestamp/identificador do pedido, conjunto de permissões e fila de pedidos
adiados. A ordem de prioridade é `(pedido_timestamp, nome)`, com
`PeerA < PeerB < PeerC`.

1. Ao pedir: incrementa o relógio, marca `DESEJADO`, limpa as permissões e envia
   chamadas unicast `receber_pedido` para os outros dois peers. Os dois unicasts
   transportam o mesmo timestamp de prioridade: são a difusão de um único pedido lógico.
2. Ao receber mensagem válida: atualiza `L = max(L, timestamp_recebido) + 1`.
3. Ao receber pedido: adia se estiver `OCUPADO` ou se estiver `DESEJADO` com pedido
   próprio de menor par `(timestamp, nome)`. Caso contrário, envia `OK` imediatamente.
4. Cada `OK` ou `LIBERACAO` válido conta uma permissão do remetente para o pedido
   identificado. Só entra com permissões dos **dois peers distintos**.
5. Ao liberar: marca `LIBERADO`, retira todos os interesses da fila daquele recurso
   e agenda uma chamada `notificar` para cada interessado. Os outros recursos não mudam.

O identificador `PeerX:Recurso:timestamp` impede que um OK antigo seja usado em
outro pedido. O conjunto de remetentes impede contar uma permissão duplicada.
Pedidos já processados do mesmo remetente/recurso também são ignorados.
`timestamp` em um OK/notificação é o instante lógico de envio; `pedido_timestamp`
identifica o pedido original ao qual a permissão pertence.

## Publisher/subscriber e concorrência

Um pedido adiado é um **registro de interesse**: o remetente é subscriber do evento
"recurso liberado" no peer que o adiou. Esse segundo peer é o publisher.
Ao liberar, ele transfere os interesses da fila para tarefas do `ThreadPoolExecutor`
e chama `notificar` em workers separados. A thread que libera o recurso não aguarda
os destinatários. O callback assinado `LIBERACAO` representa o **OK adiado**; uma
notificação sozinha não permite entrar na seção crítica.

As chamadas de rede ficam fora do `RLock`. Estado, relógio, filas e permissões são
atualizados sob esse lock. Cada worker cria um `Pyro5.api.Proxy` novo na própria
thread, evitando compartilhar proxies entre threads.
A remoção da fila e a mudança para `LIBERADO` são atômicas; depois os callbacks
são despachados. Falhas de envio são registradas e nunca viram permissões artificiais.

## Chaves e assinaturas

Cada execução gera um par RSA de 2048 bits. As chaves públicas são enviadas em PEM
aos outros dois peers e guardadas com suas referências remotas. A chave privada
fica apenas em memória. Uma chave pública cadastrada não pode ser substituída
por outra durante a execução.

`PEDIDO`, `OK` e `LIBERACAO` assinam **todo o conteúdo** em JSON canônico:
remetente, destinatário, tipo, recurso, timestamp, timestamp do pedido e identificador.
A assinatura usa RSA/PKCS1v15 com SHA-256 (`SHA256withRSA`) e viaja como texto base64.
O destinatário verifica com a chave pública do remetente antes de aplicar o protocolo.
Mensagens inválidas são descartadas e registradas.

A troca inicial de chaves é o bootstrap entre os três nomes conhecidos, sob as
premissas do exercício; não implementa uma autoridade certificadora.
`ola`, listagem de nomes e consulta de estado são diagnósticos, fora das três
mensagens do protocolo de exclusão mútua.

## Serviço de nomes e encerramento

`servico_nomes.py` conserva as funções de criação/obtenção e espera do código-base:
primeiro tenta localizar `localhost:9090`; se não encontrar, inicia o serviço numa
thread. Se outro peer ganhar a corrida pela porta, passa a usar o serviço existente.
Todos registram seu nome, fazem lookup dos outros e guardam suas URIs.

O serviço vive dentro do processo que o criou. Com todos os recursos liberados e
pedidos concluídos, encerre os outros dois peers e **o criador do serviço por último**.
A aplicação não implementa recuperação de falhas, conforme a premissa do enunciado.
Fechar à força um terminal durante a execução viola essa premissa.

Uma execução anterior pode manter registros na porta 9090. Termine os três peers
anteriores antes de iniciar novamente. O registro usa `safe=True` para não substituir
silenciosamente um peer já aberto. Para uma execução isolada, é possível passar
`--porta-ns 9091` aos **três** comandos; cada grupo deve compartilhar um único serviço.
Os testes usam uma porta temporária e não alteram uma demonstração em execução.

## Logs, testes e defesa

Cada processo mostra eventos na tela e grava `python/logs/PeerX.jsonl`.
As linhas incluem relógio, recurso/mensagem, remetente/destinatário e resultado da
verificação. Os arquivos acrescentam eventos em execuções seguintes; para guardar uma
sessão separada use `--logs outra_pasta`. Não há chaves privadas nesses logs.
O diagnóstico `log` mantém em memória os últimos 5000 eventos e mostra os 30 mais recentes.

Execute a partir de `python/`:

```bash
.venv/bin/python -m unittest discover -s tests -v
```

A suíte inclui casos determinísticos com RSA real e entrega controlada, para forçar
empates, e testes com **três processos reais**, stdin do menu e invocações Pyro5 reais.
Não usa um coordenador para conceder permissões. Verifica os cenários obrigatórios,
assinaturas, Lamport, callbacks, duplicatas, política anti-impasse, segurança das
seções críticas e progresso em rodadas concorrentes. Há teste de subida simultânea.

Leia `GUIA_EXPLICADO.md` para estudar os requisitos e os trechos reais do código.
Veja `ROTEIRO_DEFESA.md` para as falas divididas entre Yan e João, os comandos
das demonstrações e as perguntas de apoio.
Veja `validacao/RESUMO.md` e os relatórios gerados para os resultados desta entrega.
A defesa continua obrigatória para ambos os integrantes.

## Arquivos principais

- `python/peer.py`: interface remota, decisões de RA, menu e ciclo de vida.
- `python/protocolo.py`: formato, serialização canônica, RSA e dados de cada recurso.
- `python/servico_nomes.py`: criação/localização e lookup preservados do professor.
- `python/tests/`: verificação determinística e integração com processos reais.
- `python/requirements.txt`: versões usadas na validação.
- `GUIA_EXPLICADO.md`: explicação dos requisitos, do algoritmo e dos trechos reais do código.
- `ROTEIRO_DEFESA.md`: falas de Yan e João, demonstrações e perguntas para a apresentação.

## Referências

- Enunciado `Trab1_T1_Exclusao_Mutua_P2P_atualizado2026.pdf`, seções 3 a 10.
- Material das aulas 05, 06, 07 e 08 e da Atividade 01, indicado pelo professor.
- Ricart, G.; Agrawala, A. K. *An optimal algorithm for mutual exclusion in computer networks*, 1981.
- [Pyro5: chamadas remotas e propriedade de proxies por thread](https://pyro5.readthedocs.io/en/latest/clientcode.html).
- [cryptography: geração, assinatura e verificação RSA](https://cryptography.io/en/latest/hazmat/primitives/asymmetric/rsa/).
