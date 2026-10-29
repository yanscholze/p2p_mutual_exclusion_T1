package peer;

import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * Processo par (peer). O MESMO programa é executado três vezes, cada uma com
 * um nome diferente:
 *
 *   java peer.Peer PeerA
 *   java peer.Peer PeerB
 *   java peer.Peer PeerC
 *
 * Passos executados no main:
 *   1. cria OU obtém o serviço de nomes (só um processo consegue criá-lo);
 *   2. instancia a classe servente (PeerImpl) e a registra com o seu nome;
 *   3. espera os outros dois peers aparecerem no serviço de nomes;
 *   4. faz lookup de cada um e guarda as referências remotas;
 *   5. abre o menu de interação.
 */
public class Peer {

    /** Porta padrão do rmiregistry. */
    private static final int PORTA_SN = 1099;

    /** Nomes conhecidos de todos os peers (enunciado: cada peer conhece os demais). */
    private static final List<String> TODOS_OS_PEERS = Arrays.asList("PeerA", "PeerB", "PeerC");

    public static void main(String[] args) throws Exception {
        try {
            System.setOut(new PrintStream(System.out, true, "UTF-8"));
        } catch (Exception e) {
            // segue com o encoding padrão se UTF-8 não estiver disponível
        }

        if (args.length != 1 || !TODOS_OS_PEERS.contains(args[0])) {
            System.out.println("Uso: java peer.Peer <PeerA|PeerB|PeerC>");
            return;
        }
        String meuNome = args[0];
        System.out.println("[" + meuNome + "] PID deste processo: " + ProcessHandle.current().pid());

        // 1. Serviço de nomes: só UM processo consegue criá-lo na porta 1099.
        Registry registry = obterOuCriarServicoDeNomes(meuNome);

        // 2. Objeto remoto deste peer, registrado com o seu nome.
        PeerImpl meuPeer = new PeerImpl(meuNome);
        registry.rebind(meuNome, meuPeer);
        System.out.println("[" + meuNome + "] registrado no serviço de nomes.");

        // 3 e 4. Espera os demais e guarda as referências remotas.
        List<String> outros = new ArrayList<>(TODOS_OS_PEERS);
        outros.remove(meuNome);
        for (String nomeOutro : outros) {
            InterfacePeer ref = aguardarPeer(registry, nomeOutro, meuNome);
            meuPeer.adicionarPeer(nomeOutro, ref);
            System.out.println("[" + meuNome + "] referência de " + nomeOutro + " obtida.");
        }

        // TODO (T1): gerar o par de chaves (KeyPairGenerator) e enviar a chave
        // pública para cada um dos outros peers.

        menu(meuPeer, registry);
    }

    /**
     * Tenta CRIAR o rmiregistry; se a porta já estiver ocupada por um registry
     * criado por outro peer, createRegistry lança ExportException e então
     * apenas OBTEMOS a referência do que já existe.
     *
     * Observação: o registry vive dentro do processo que o criou. O enunciado
     * assume que não há falhas, mas se você encerrar esse peer primeiro, o
     * serviço de nomes some junto com ele.
     */
    private static Registry obterOuCriarServicoDeNomes(String meuNome) throws RemoteException {
        try {
            Registry r = LocateRegistry.createRegistry(PORTA_SN);
            System.out.println("[" + meuNome + "] serviço de nomes CRIADO na porta " + PORTA_SN + ".");
            return r;
        } catch (ExportException jaExiste) {
            Registry r = LocateRegistry.getRegistry(PORTA_SN);
            System.out.println("[" + meuNome + "] serviço de nomes já existia — referência obtida.");
            return r;
        }
    }

    /** Fica consultando o serviço de nomes até o peer indicado se registrar. */
    private static InterfacePeer aguardarPeer(Registry registry, String nomeOutro, String meuNome)
            throws RemoteException, InterruptedException {
        boolean avisou = false;
        while (true) {
            try {
                return (InterfacePeer) registry.lookup(nomeOutro);
            } catch (NotBoundException aindaNao) {
                if (!avisou) {
                    System.out.println("[" + meuNome + "] aguardando " + nomeOutro + " entrar...");
                    avisou = true;
                }
                Thread.sleep(500);
            }
        }
    }

    /**
     * Menu de interação mínimo.
     * TODO (T1): acrescente as opções do trabalho (pedir R1/R2, liberar R1/R2,
     * mostrar estado e filas de cada recurso...).
     */
    private static void menu(PeerImpl meuPeer, Registry registry) throws Exception {
        Scanner in = new Scanner(System.in, "UTF-8");
        String nome = meuPeer.getNome();
        while (true) {
            System.out.println();
            System.out.println("==== " + nome + " ====");
            System.out.println("1 - Enviar olá para os outros peers (unicast)");
            System.out.println("2 - Listar nomes no serviço de nomes (list)");
            System.out.println("0 - Sair");
            System.out.print("> ");
            if (!in.hasNextLine()) {
                break;
            }
            String op = in.nextLine().trim();
            switch (op) {
                case "1":
                    meuPeer.enviarOlaParaTodos();
                    break;
                case "2":
                    try {
                        System.out.println("Registrados: " + Arrays.toString(registry.list()));
                    } catch (RemoteException e) {
                        System.out.println("Serviço de nomes indisponível: " + e.getMessage());
                    }
                    break;
                case "0":
                    try {
                        registry.unbind(nome);
                    } catch (Exception semServicoDeNomes) {
                        // o peer que hospedava o serviço de nomes já saiu
                    }
                    UnicastRemoteObject.unexportObject(meuPeer, true);
                    System.out.println("[" + nome + "] encerrado.");
                    System.exit(0);
                    break;
                default:
                    System.out.println("Opção inválida.");
            }
        }
    }
}
