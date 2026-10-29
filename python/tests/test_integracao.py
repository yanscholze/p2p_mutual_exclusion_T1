"""Teste ponta a ponta: três processos e chamadas Pyro5 reais em porta isolada.

Os pedidos vêm pelo stdin do menu. consultar_estado é somente leitura.
O programa de testes observa o protocolo; não concede permissões aos peers.
"""

import concurrent.futures
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import Pyro5.api
import Pyro5.errors

from protocolo import PEERS, RECURSOS

PASTA = Path(__file__).resolve().parents[1]


class ClusterPyro:
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix="t1-p2p-")
        cls.pasta = Path(cls.tmp.name)
        with socket.socket() as sock:
            sock.bind(("localhost", 0))
            cls.porta = sock.getsockname()[1]
        cls.processos = {}
        cls.saidas = {}
        cls.uris = {}
        try:
            cls.iniciar("PeerA")
            if not getattr(cls, "inicio_concorrente", False):
                # Sequencial para sabermos que A deve ser encerrado por último.
                cls.esperar(lambda: cls.registrado("PeerA"), "registro de A")
            cls.iniciar("PeerB")
            cls.iniciar("PeerC")
            cls.esperar(lambda: all(cls.registrado(n) for n in PEERS), "registros B/C")
            cls.esperar(lambda: all(cls.estado(n)["pronto"] for n in PEERS), "troca de chaves")
        except Exception:
            cls.encerrar()
            raise

    @classmethod
    def iniciar(cls, nome):
        cls.saidas[nome] = (cls.pasta / f"{nome}.stdout").open("w+")
        cls.processos[nome] = subprocess.Popen(
            [sys.executable, "-u", str(PASTA / "peer.py"), nome,
             "--porta-ns", str(cls.porta), "--logs", str(cls.pasta / "logs")],
            cwd=PASTA, stdin=subprocess.PIPE, stdout=cls.saidas[nome],
            stderr=subprocess.STDOUT, text=True, env={**os.environ, "PYTHONUNBUFFERED": "1"})

    @classmethod
    def registrado(cls, nome):
        try:
            with Pyro5.api.locate_ns(host="localhost", port=cls.porta) as ns:
                cls.uris[nome] = ns.lookup(nome)
            return True
        except Pyro5.errors.PyroError:
            return False

    @classmethod
    def estado(cls, nome, eventos=False):
        with Pyro5.api.Proxy(cls.uris[nome]) as p:
            p._pyroTimeout = 2
            return p.consultar_estado(eventos)

    @classmethod
    def esperar(cls, condicao, descricao, timeout=12):
        limite = time.monotonic() + timeout
        while time.monotonic() < limite:
            if condicao():
                return
            time.sleep(0.025)
        raise AssertionError(f"tempo excedido: {descricao}; saídas em {cls.pasta}")

    @classmethod
    def comando(cls, nome, comando):
        cls.processos[nome].stdin.write(comando + "\n")
        cls.processos[nome].stdin.flush()

    @classmethod
    def ocupar(cls, nome, recurso):
        cls.comando(nome, f"pedir {recurso}")
        cls.esperar(lambda: cls.estado(nome)["recursos"][recurso]["estado"] == "OCUPADO",
                    f"{nome} ocupar {recurso}")

    @classmethod
    def liberar(cls, nome, recurso):
        cls.comando(nome, f"liberar {recurso}")
        cls.esperar(lambda: cls.estado(nome)["recursos"][recurso]["estado"] == "LIBERADO",
                    f"{nome} liberar {recurso}")

    @classmethod
    def encerrar(cls):
        for nome in ("PeerC", "PeerB", "PeerA"):
            proc = cls.processos.get(nome)
            if not proc:
                continue
            if proc.poll() is None:
                try:
                    cls.comando(nome, "sair")
                    proc.wait(timeout=5)
                except (subprocess.TimeoutExpired, BrokenPipeError):
                    proc.terminate()
                    proc.wait(timeout=5)
            if proc.stdin:
                proc.stdin.close()
        for f in cls.saidas.values():
            f.flush()
            f.seek(0)
            saida = f.read()
            if "Traceback" in saida:
                print(saida)
            f.close()

    @classmethod
    def tearDownClass(cls):
        cls.encerrar()
        # Relatório JSON contém eventos reais, sem chaves privadas.
        relatorio = {"tipo": "Pyro5 real, três processos", "porta_isolada": cls.porta,
                     "retornos": {n: p.returncode for n, p in cls.processos.items()},
                     "eventos": {}}
        for nome in PEERS:
            caminho = cls.pasta / "logs" / f"{nome}.jsonl"
            if caminho.exists():
                relatorio["eventos"][nome] = [json.loads(l) for l in caminho.read_text().splitlines()]
        destino = os.environ.get("T1_RELATORIO_TESTE")
        if destino:
            if getattr(cls, "inicio_concorrente", False):
                p = Path(destino)
                destino = p.with_name(p.stem + "_inicio_concorrente" + p.suffix)
            Path(destino).write_text(json.dumps(relatorio, ensure_ascii=False, indent=2))
        cls.tmp.cleanup()

    def tearDown(self):
        # Detecta sobreposição pelos eventos em todos os processos, além de snapshots.
        eventos = sorted([e for n in PEERS for e in self.estado(n, True)["eventos"]
                          if e["evento"] in ("ENTROU_SC", "SAIU_SC")],
                         key=lambda e: e["instante_ns"])
        ocupantes = {}
        for e in eventos:
            recurso = e["recurso"]
            if e["evento"] == "ENTROU_SC":
                self.assertNotIn(recurso, ocupantes, f"sobreposição: {e}")
                self.assertEqual(len(e["permissoes"]), 2)
                ocupantes[recurso] = e["peer"]
            else:
                self.assertEqual(ocupantes.pop(recurso), e["peer"])
        self.assertFalse(ocupantes)


class IntegracaoTest(ClusterPyro, unittest.TestCase):
    def test_01_inicializacao_e_acesso_sem_concorrencia(self):
        for nome in PEERS:
            s = self.estado(nome)
            self.assertEqual(s["referencias"], sorted(set(PEERS) - {nome}))
            self.assertEqual(s["chaves_publicas"], s["referencias"])
        self.ocupar("PeerA", "R1")
        self.liberar("PeerA", "R1")

    def test_02_dois_pedidos_enquanto_terceiro_ocupa(self):
        self.ocupar("PeerC", "R1")
        self.comando("PeerB", "pedir R1")
        self.esperar(lambda: len(self.estado("PeerC")["recursos"]["R1"]["fila"]) == 1,
                    "primeiro interesse")
        self.esperar(lambda: any(e["evento"] == "RECEBIDA" and
                                 e["mensagem"]["tipo"] == "PEDIDO" and
                                 e["mensagem"]["remetente"] == "PeerB"
                                 for e in self.estado("PeerA", True)["eventos"]),
                    "A observar pedido anterior de B")
        self.comando("PeerA", "pedir R1")
        self.esperar(lambda: len(self.estado("PeerC")["recursos"]["R1"]["fila"]) == 2,
                    "segundo interesse")
        fila = self.estado("PeerC")["recursos"]["R1"]["fila"]
        ordem = [m["remetente"] for m in fila]
        self.assertEqual(ordem[0], "PeerB")  # B happened-before A via mensagens.
        self.liberar("PeerC", "R1")
        for nome in ordem:
            self.esperar(lambda: self.estado(nome)["recursos"]["R1"]["estado"] == "OCUPADO",
                        f"ordem de prioridade: {nome}")
            outro = next(n for n in ordem if n != nome)
            self.assertNotEqual(self.estado(outro)["recursos"]["R1"]["estado"], "OCUPADO")
            self.liberar(nome, "R1")
        self.assertEqual(self.estado("PeerC")["recursos"]["R1"]["fila"], [])
        notificacoes = [e for e in self.estado("PeerC", True)["eventos"]
                        if e["evento"] == "ENVIADA" and e["mensagem"]["tipo"] == "LIBERACAO"]
        self.assertGreaterEqual(len(notificacoes), 2)

    def test_03_recursos_simultaneos_e_politica_anti_impasse(self):
        self.ocupar("PeerA", "R1")
        self.ocupar("PeerB", "R2")
        self.assertEqual(self.estado("PeerA")["recursos"]["R1"]["estado"], "OCUPADO")
        self.comando("PeerC", "pedir R1")
        self.esperar(lambda: self.estado("PeerC")["recursos"]["R1"]["estado"] == "DESEJADO",
                    "C esperar R1")
        self.comando("PeerC", "pedir R2")
        self.esperar(lambda: any(e["evento"] == "COMANDO_RECUSADO" and
                                 "espere R1" in e["motivo"]
                                 for e in self.estado("PeerC", True)["eventos"]),
                    "C não adquirir R2 antes de receber R1")
        self.assertEqual(self.estado("PeerC")["recursos"]["R2"]["estado"], "LIBERADO")
        antes = len([e for e in self.estado("PeerB", True)["eventos"]
                     if e["evento"] == "COMANDO_RECUSADO"])
        self.comando("PeerB", "pedir R1")
        self.esperar(lambda: len([e for e in self.estado("PeerB", True)["eventos"]
                                  if e["evento"] == "COMANDO_RECUSADO"]) > antes,
                    "recusa de inversão R2 -> R1")
        self.liberar("PeerB", "R2")
        self.ocupar("PeerA", "R2")
        self.assertTrue(all(r["estado"] == "OCUPADO"
                            for r in self.estado("PeerA")["recursos"].values()))
        self.liberar("PeerA", "R2")
        self.liberar("PeerA", "R1")
        self.esperar(lambda: self.estado("PeerC")["recursos"]["R1"]["estado"] == "OCUPADO",
                    "C obter R1 depois da liberação")
        self.liberar("PeerC", "R1")

    def test_04_assinatura_adulterada_rejeitada_pelo_destinatario(self):
        antes = self.estado("PeerB")
        eventos = len(self.estado("PeerB", True)["eventos"])
        self.comando("PeerA", "adulterar PeerB R1")
        self.esperar(lambda: any(e["evento"] == "MENSAGEM_REJEITADA" and
                                 e.get("assinatura") == "INVALIDA"
                                 for e in self.estado("PeerB", True)["eventos"][eventos:]),
                    "verificação RSA rejeitar mensagem adulterada")
        self.assertEqual(antes, self.estado("PeerB"))

    def test_05_pedidos_concorrentes_repetidos_sem_inanicao(self):
        for recurso in RECURSOS:
            for rodada in range(3):
                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                    list(executor.map(lambda n: self.comando(n, f"pedir {recurso}"), PEERS))
                restantes = set(PEERS)
                while restantes:
                    def ha_ocupante():
                        return any(self.estado(n)["recursos"][recurso]["estado"] == "OCUPADO"
                                   for n in restantes)
                    self.esperar(ha_ocupante, f"progresso {recurso}, rodada {rodada}")
                    ocupantes = [n for n in restantes
                                 if self.estado(n)["recursos"][recurso]["estado"] == "OCUPADO"]
                    self.assertEqual(len(ocupantes), 1)
                    self.liberar(ocupantes[0], recurso)
                    restantes.remove(ocupantes[0])
        for n in PEERS:
            self.assertTrue(all(r["estado"] == "LIBERADO" and not r["fila"]
                                for r in self.estado(n)["recursos"].values()))

    def test_06_menu_estado_log_e_ola(self):
        for cmd in ("estado", "log", "nomes", "ola"):
            self.comando("PeerA", cmd)
        self.esperar(lambda: any(e["evento"] == "OLA" for e in self.estado("PeerB", True)["eventos"]),
                    "olá pelo menu")


class InicioConcorrenteTest(ClusterPyro, unittest.TestCase):
    inicio_concorrente = True

    def test_criacao_concorrente_do_servico_de_nomes(self):
        # Todos iniciaram sem esperar um serviço previamente criado.
        self.assertTrue(all(self.estado(n)["pronto"] for n in PEERS))
        self.ocupar("PeerC", "R2")
        self.liberar("PeerC", "R2")


if __name__ == "__main__":
    unittest.main(verbosity=2)
