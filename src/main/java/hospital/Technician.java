package hospital;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class Technician {
    private final static String EXAMINATION_EXCHANGE = "examination";




    public static void main(String[] args) throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXAMINATION_EXCHANGE,"topic");




        ExaminationType examinationOne;
        ExaminationType examinationTwo;

        try(Scanner sc = new Scanner(System.in)){
            String line = sc.nextLine();
            String[] availableExaminationTypes = line.split(" ");

            if (availableExaminationTypes.length != 2) {
                System.out.println("Unknown command");
                return;
            }

            try {
                examinationOne = ExaminationType.valueOf(availableExaminationTypes[0].toUpperCase());
                examinationTwo = ExaminationType.valueOf(availableExaminationTypes[1].toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid examination type: " + availableExaminationTypes[0] + " or " + availableExaminationTypes[1]);
                return;
            }

        }

        String exQueueOne = channel.queueDeclare(
                examinationOne.name(),
                false,
                false,
                false,
                null
        ).getQueue();
        String exQueueTwo = channel.queueDeclare(
                examinationTwo.name(),
                false,
                false,
                false,
                null
        ).getQueue();

        System.out.println(exQueueOne + " " + exQueueTwo);

        channel.queueBind(exQueueOne,EXAMINATION_EXCHANGE,examinationOne.toString());
        channel.queueBind(exQueueTwo,EXAMINATION_EXCHANGE,examinationTwo.toString());


        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("Received: " + delivery.getEnvelope().getRoutingKey() + ": " + message);
        };

        channel.basicConsume(exQueueOne,true,deliverCallback,consumerTag -> {});
        channel.basicConsume(exQueueTwo,true,deliverCallback,consumerTag -> {});

    }
}
