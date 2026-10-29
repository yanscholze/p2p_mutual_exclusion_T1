# Validação da implementação

Execução local em Fedora/Linux com Python 3.14.9, Pyro5 5.17 e cryptography 50.0.2.
Código escolhido: Python/Pyro5. Data da entrega: 28/10/2026.

**Resultado: 20 testes aprovados.** Nas duas sessões Pyro5, os seis subprocessos
encerraram com código 0. Foram registradas 27 entradas e 27 saídas de seção crítica,
sem sobreposição por recurso e sem erro de envio. Uma mensagem adulterada foi
rejeitada na sessão principal.

O arquivo `resultado_testes.txt` contém a saída do unittest. Os relatórios JSON
registram eventos efetivos das sessões Pyro5 executadas pelos testes; não são
mensagens inventadas para uma apresentação. São evidências de execução finita,
e não substituem a defesa nem constituem prova geral para toda execução possível.

## O que foi exercitado

- 13 testes determinísticos: assinaturas cobrindo todos os campos; empate de
  timestamp resolvido pelo nome; timestamp prevalecendo sobre nome; atualização
  de Lamport; notificação única e duplicata insuficientes; OK antigo ignorado;
  pedidos/OKs/liberações inválidos descartados sem alterar estado; formatos inválidos;
  recursos independentes; ordem contra impasse; barreira de chaves; substituição
  de chave recusada; retorno da liberação com worker bloqueado num subscriber lento;
  recusa de R2 enquanto o peer ainda espera R1.
- 6 cenários com três processos reais: descoberta/chaves e acesso sem concorrência;
  dois peers esperando um terceiro com notificações assinadas; dois recursos
  simultâneos e bloqueio de inversão de ordem; assinatura adulterada rejeitada;
  seis rodadas de pedidos dos três peers (três por recurso); comandos de diagnóstico.
- 1 cenário com os três processos iniciados simultaneamente, sem serviço de nomes
  previamente criado na porta de teste; todos chegam ao estado pronto e usam R2.

Nos testes de integração, os comandos são enviados ao stdin dos menus. Consultas
remotas apenas observam o estado. As permissões são concedidas pelos próprios
peers através dos handlers assinados, em sockets Pyro5 reais.
Os eventos de entrada/saída são conferidos para detectar sobreposição por recurso,
e toda entrada deve mostrar dois remetentes de permissão distintos.

A suíte usa portas temporárias e encerra apenas os subprocessos que ela própria criou.
O teste determinístico de subscriber lento controla a entrega em memória;
a suíte de integração verifica o callback real entre processos.

## Reproduzir

Na pasta `python`, depois de `bash preparar.sh`:

```bash
T1_RELATORIO_TESTE=../validacao/eventos_integracao.json \
  .venv/bin/python -m unittest discover -s tests -v
```

Sem a variável `T1_RELATORIO_TESTE`, os testes não substituem os relatórios incluídos.
Falhas de processo/canal e recuperação não fazem parte das premissas do professor.
As demonstrações interativas para a defesa estão em `ROTEIRO_DEFESA.md`.
