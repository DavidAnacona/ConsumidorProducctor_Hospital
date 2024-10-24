package Productor;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import javax.swing.table.DefaultTableModel;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import org.json.JSONObject;

public class Productor {

    private final static String QUEUE_NAME = "topic_solicitud"; 
    private final static String HOST = "127.0.0.1";

    /**
     * Método para enviar un mensaje a la cola de solicitudes.
     */
    public static void enviarSolicitudACola(String nombreSolicitud, String tipoSolicitud, String estadoSolicitud, DefaultTableModel tableModel) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            JSONObject json = new JSONObject();
            json.put("nombre", nombreSolicitud);
            json.put("tipo", tipoSolicitud);
            
            channel.basicPublish("", QUEUE_NAME, null, nombreSolicitud.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Solicitud: '" + nombreSolicitud + " agregada a la cola");
            tableModel.addRow(new Object[]{nombreSolicitud, tipoSolicitud, estadoSolicitud});
            GetResponse response;
            System.out.println("\nContenido de la cola:");
            boolean hasMoreMessages = true;
            
            while (hasMoreMessages) {
                response = channel.basicGet(QUEUE_NAME, true);  // true = auto-acknowledge
                if (response != null) {
                    String mensajeCola = new String(response.getBody(), StandardCharsets.UTF_8);
                    System.out.println(" - " + mensajeCola);
                } else {
                    hasMoreMessages = false; 
                }
            }
            System.out.println("[x] No hay más solicitudes en la cola.");
        }
    }
}
