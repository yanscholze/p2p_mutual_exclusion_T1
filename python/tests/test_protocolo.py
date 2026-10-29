"""Casos determinísticos: RSA real e entrega controlada para forçar os empates.

A rede em memória permite decidir a ordem exata das mensagens. A outra suíte
(test_integracao.py) usa os mesmos handlers em três processos com Pyro5 real.
"""

import copy
import threading
import unittest
from collections import deque
from concurrent.futures import ThreadPoolExecutor

from peer import Peer
from protocolo import PEERS, assinar, carregar_chave, verificar


class RedeControlada:
    """Captura os workers; não substitui chaves, assinaturas ou decisões de RA."""

    def __init__(self):
        self.pendentes = deque()
        self.lock = threading.Lock()
        self.peers = {nome: Peer(nome, silencioso=True) for nome in PEERS}
        for nome, peer in self.peers.items():
            peer.outros_peers = {n: n for n in PEERS if n != nome}
            for n, outro in self.peers.items():
                if n != nome:
                    peer.receber_chave_publica(n, outro._pem)
            peer._chaves_enviadas = True
            peer._workers.shutdown(wait=True)
            peer._workers = ThreadPoolExecutor(max_workers=1)
            peer._transmitir = self.capturar

    def capturar(self, metodo, envelope):
        with self.lock:
            self.pendentes.append((metodo, envelope))

    def drenar(self):
        # Barreira dos workers antes de ler a fila: nenhuma espera de rede.
        for _ in range(20):
            for peer in self.peers.values():
                peer._workers.submit(lambda: None).result(timeout=2)
            with self.lock:
                lote = list(self.pendentes)
                self.pendentes.clear()
            if not lote:
                # Todos os callbacks do lote anterior já concluíram antes da barreira.
                return
            for metodo, envelope in lote:
                alvo = self.peers[envelope["conteudo"]["destinatario"]]
                getattr(alvo, metodo)(envelope)
        raise AssertionError("rede não ficou ociosa")

    def fechar(self):
        for peer in self.peers.values():
            peer.fechar()


class ProtocoloTest(unittest.TestCase):
    def setUp(self):
        self.rede = RedeControlada()
        self.a, self.b, self.c = (self.rede.peers[n] for n in PEERS)

    def tearDown(self):
        self.rede.fechar()

    def mensagem(self, emissor, destino, tipo="PEDIDO", recurso="R1", ts=10, pedido_ts=None):
        origem = emissor.nome if tipo == "PEDIDO" else destino.nome
        pedido_ts = pedido_ts or ts
        return assinar(emissor._chave_privada, {
            "remetente": emissor.nome, "destinatario": destino.nome,
            "tipo": tipo, "recurso": recurso, "timestamp": ts,
            "pedido_timestamp": pedido_ts,
            "pedido_id": f"{origem}:{recurso}:{pedido_ts}"})

    def test_assinatura_cobre_todos_os_campos(self):
        envelope = self.mensagem(self.a, self.b)
        chave = carregar_chave(self.a._pem)
        verificar(chave, envelope)
        for campo, valor in envelope["conteudo"].items():
            adulterada = copy.deepcopy(envelope)
            adulterada["conteudo"][campo] = valor + 1 if type(valor) is int else valor + "x"
            with self.subTest(campo=campo), self.assertRaises(Exception):
                verificar(chave, adulterada)

    def test_empate_de_timestamp_eh_desfeito_pelo_id(self):
        # Todos criam pedido antes de qualquer entrega: timestamp=1 para os três.
        for p in (self.c, self.b, self.a):
            p.solicitar("R1")
        self.rede.drenar()
        self.assertEqual(self.a.recursos["R1"].estado, "OCUPADO")
        self.assertEqual(self.b.recursos["R1"].estado, "DESEJADO")
        self.assertEqual(self.c.recursos["R1"].estado, "DESEJADO")
        self.a.liberar("R1")
        self.rede.drenar()
        self.assertEqual(self.b.recursos["R1"].estado, "OCUPADO")
        self.assertEqual(self.c.recursos["R1"].estado, "DESEJADO")
        self.b.liberar("R1")
        self.rede.drenar()
        self.assertEqual(self.c.recursos["R1"].estado, "OCUPADO")
        self.c.liberar("R1")
        self.assertTrue(all(p.pode_sair() for p in self.rede.peers.values()))

    def test_timestamp_tem_prioridade_sobre_id(self):
        self.a.relogio = 20
        self.a.solicitar("R1")
        self.b.solicitar("R1")  # B tem timestamp 1, embora A tenha id menor.
        self.rede.drenar()
        self.assertEqual(self.b.recursos["R1"].estado, "OCUPADO")
        self.assertEqual(self.a.recursos["R1"].estado, "DESEJADO")

    def test_relogio_na_recepcao(self):
        self.a.relogio = 8
        self.a.receber_pedido(self.mensagem(self.b, self.a, ts=50))
        recebida = [e for e in self.a.consultar_estado(True)["eventos"]
                    if e["evento"] == "RECEBIDA"][-1]
        self.assertEqual(recebida["relogio"], 51)

    def test_uma_notificacao_nao_basta_e_duplicata_nao_conta(self):
        self.a.solicitar("R1")
        env = self.mensagem(self.b, self.a, "LIBERACAO", ts=10, pedido_ts=1)
        self.a.notificar(env)
        self.a.notificar(env)
        self.assertEqual(self.a.recursos["R1"].estado, "DESEJADO")
        self.assertEqual(self.a.recursos["R1"].permissoes, {"PeerB"})
        self.a.receber_ok(self.mensagem(self.c, self.a, "OK", ts=11, pedido_ts=1))
        self.assertEqual(self.a.recursos["R1"].estado, "OCUPADO")

    def test_permissao_antiga_nao_entra_em_novo_pedido(self):
        self.a.solicitar("R1")
        self.a.receber_ok(self.mensagem(self.b, self.a, "OK", ts=20, pedido_ts=2))
        self.assertEqual(self.a.recursos["R1"].permissoes, set())

    def test_invalida_eh_descartada_sem_mudar_relogio_estado_ou_fila(self):
        for tipo, metodo in (("PEDIDO", "receber_pedido"), ("OK", "receber_ok"),
                              ("LIBERACAO", "notificar")):
            with self.subTest(tipo=tipo):
                antes = self.a.consultar_estado()
                env = self.mensagem(self.b, self.a, tipo)
                env["assinatura"] = "AAAA"
                self.assertFalse(getattr(self.a, metodo)(env))
                self.assertEqual(antes, self.a.consultar_estado())
                self.assertEqual(self.a.consultar_estado(True)["eventos"][-1]["assinatura"],
                                 "INVALIDA")

    def test_formato_invalido_nao_derruba_handler(self):
        for env in (None, [], {}, {"conteudo": []}, {"conteudo": {}, "assinatura": 1}):
            self.assertFalse(self.a.receber_pedido(env))
        self.assertEqual(self.a.relogio, 0)

    def test_recursos_independentes_e_ordem_contra_impasse(self):
        self.a.solicitar("R1")
        self.b.solicitar("R2")
        self.rede.drenar()
        self.assertEqual(self.a.recursos["R1"].estado, "OCUPADO")
        self.assertEqual(self.b.recursos["R2"].estado, "OCUPADO")
        with self.assertRaisesRegex(ValueError, "anti-impasse"):
            self.b.solicitar("R1")
        self.b.liberar("R2")
        self.a.solicitar("R2")
        self.rede.drenar()
        self.assertEqual([r.estado for r in self.a.recursos.values()], ["OCUPADO"] * 2)

    def test_pedir_antes_das_chaves_e_liberar_sem_ocupar_sao_recusados(self):
        self.a._chaves_enviadas = False
        with self.assertRaises(ValueError):
            self.a.solicitar("R1")
        with self.assertRaises(ValueError):
            self.a.liberar("R1")

    def test_substituicao_de_chave_publica_recusada(self):
        self.assertFalse(self.a.receber_chave_publica("PeerB", self.c._pem))
        self.assertFalse(self.a.receber_chave_publica("Invasor", self.b._pem))

    def test_nao_pode_adquirir_r2_enquanto_ainda_espera_r1(self):
        # Só ordenar envios não basta: R2 poderia ser concedido antes de R1.
        self.a.solicitar("R1")
        with self.assertRaisesRegex(ValueError, "espere R1"):
            self.a.solicitar("R2")
        self.assertEqual(self.a.recursos["R2"].estado, "LIBERADO")

    def test_liberacao_retorna_enquanto_callback_esta_bloqueado(self):
        self.a.solicitar("R1")
        self.rede.drenar()
        self.b.solicitar("R1")
        self.rede.drenar()
        iniciou = threading.Event()
        continuar = threading.Event()

        def subscriber_lento(metodo, env):
            iniciou.set()
            continuar.wait(3)
        self.a._transmitir = subscriber_lento
        try:
            # Retorna mesmo que o worker não possa terminar sua chamada ao subscriber.
            self.a.liberar("R1")
            self.assertTrue(iniciou.wait(1))
            self.assertFalse(continuar.is_set())
            self.assertEqual(self.a.recursos["R1"].estado, "LIBERADO")
            self.assertEqual(self.a.recursos["R1"].adiados, {})
        finally:
            continuar.set()


if __name__ == "__main__":
    unittest.main(verbosity=2)
