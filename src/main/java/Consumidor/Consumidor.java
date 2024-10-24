package Consumidor;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import javax.swing.table.DefaultTableModel;

/**
 *
 * @author davii
 */
public class Consumidor {

    private final static String QUEUE_REPARTIDORES = "topic_repartidores";
    private final static String QUEUE_SOLICITUDES = "topic_solicitud";
    private final static String HOST = "127.0.0.1";

    public static void agregarRepartidor(String nombreRepartidor, DefaultTableModel tableModel) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            // Publicar el repartidor en la cola de repartidores
            String mensajeRepartidor = nombreRepartidor;
            channel.queueDeclare(QUEUE_REPARTIDORES, false, false, false, null);
            channel.basicPublish("", QUEUE_REPARTIDORES, null, mensajeRepartidor.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Repartidor agregado a la cola: " + nombreRepartidor);

            // Buscar si hay solicitudes pendientes en la cola de solicitudes
            
            String solicitud = consultarSolicitudes();
            if (solicitud != null) {
                System.out.println("Cola de solicitud: " + solicitud);
                // Si hay una solicitud, asignarla al repartidor
                System.out.println(" [x] Solicitud asignada: " + solicitud);
                tableModel.addRow(new Object[]{solicitud, nombreRepartidor, "Gestionando"});
            } else {
                System.out.println(" [x] No hay solicitudes en la cola.");
            }
        }
    }
    
    public static String consultarSolicitudes() throws IOException, TimeoutException{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        String solicitud = null;
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_SOLICITUDES, false, false, false, null);
            GetResponse response;
            boolean HaySolicitudes = true;
            while(HaySolicitudes){
                response = channel.basicGet(QUEUE_SOLICITUDES, true);
                if(response != null){
                    solicitud = new String(response.getBody(), StandardCharsets.UTF_8);  
                }else{
                    HaySolicitudes = false;
                }
            }
        }
        return solicitud;
    };
}
