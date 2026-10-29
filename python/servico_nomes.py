"""Criação/obtenção do serviço de nomes e lookup do esqueleto do professor.

A lógica original foi mantida; a porta pode ser alterada em conjunto nos três
peers para isolar uma execução de testes de uma demonstração já aberta.
"""
import threading
import time
import Pyro5.api
import Pyro5.errors
import Pyro5.nameserver

HOST_NS = "localhost"
PORTA_NS = 9090

def obter_ou_criar_servico_de_nomes(meu_nome):
    """
    Tenta localizar o serviço de nomes; se não existir, cria um nesta mesma
    máquina, rodando numa thread deste processo. Se dois peers tentarem criar
    ao mesmo tempo, o segundo não consegue usar a porta (OSError) e passa a
    apenas localizar o serviço criado pelo primeiro.

    Observação: o serviço de nomes vive dentro do processo que o criou. Se
    você encerrar esse peer primeiro, o serviço de nomes some junto.
    """
    try:
        ns = Pyro5.api.locate_ns(host=HOST_NS, port=PORTA_NS)
        print(f"[{meu_nome}] serviço de nomes já existia — referência obtida.")
        return ns
    except Pyro5.errors.NamingError:
        pass

    erro_ao_criar = []

    def rodar_ns():
        try:
            Pyro5.nameserver.start_ns_loop(host=HOST_NS, port=PORTA_NS,
                                           enableBroadcast=False)
        except OSError as e:  # porta já ocupada por outro peer
            erro_ao_criar.append(e)

    threading.Thread(target=rodar_ns, daemon=True).start()

    for _ in range(50):  # até ~5 s para o serviço de nomes subir
        time.sleep(0.1)
        try:
            ns = Pyro5.api.locate_ns(host=HOST_NS, port=PORTA_NS)
            if erro_ao_criar:
                print(f"[{meu_nome}] outro peer criou o serviço de nomes antes — referência obtida.")
            else:
                print(f"[{meu_nome}] serviço de nomes CRIADO em {HOST_NS}:{PORTA_NS}.")
            return ns
        except Pyro5.errors.NamingError:
            continue
    raise RuntimeError("não foi possível localizar nem criar o serviço de nomes")


def aguardar_peer(ns, nome_outro, meu_nome):
    """Fica consultando o serviço de nomes até o peer indicado se registrar."""
    avisou = False
    while True:
        try:
            return ns.lookup(nome_outro)  # devolve a URI do objeto remoto
        except Pyro5.errors.NamingError:
            if not avisou:
                print(f"[{meu_nome}] aguardando {nome_outro} entrar...")
                avisou = True
            time.sleep(0.5)


