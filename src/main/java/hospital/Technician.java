package hospital;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Technician {
    private final static String EXAMINATION_EXCHANGE = "examination";
    private final static String RESULTS_EXCHANGE = "results";

    private final Channel channel;

    private ExaminationType examinationOne;
    private ExaminationType examinationTwo;

    private String exQueueOne;
    private String exQueueTwo;

    public Technician() throws Exception {
        channel = createChannel();
        declareExchanges();
    }

    private Channel createChannel() throws IOException, TimeoutException {
        final Channel channel;
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        channel = connection.createChannel();
        return channel;
    }

    private void declareExchanges() throws IOException {
        channel.exchangeDeclare(EXAMINATION_EXCHANGE, "topic");
        channel.exchangeDeclare(RESULTS_EXCHANGE, "direct");
    }

    private void setAvailableExaminationTypes() {
        System.out.print("Enter examination types separated by space: ");
        try (Scanner sc = new Scanner(System.in)) {
            String line = sc.nextLine();
            String[] availableExaminationTypes = line.split(" ");

            if (availableExaminationTypes.length != 2) {
                System.out.println("Usage: <ExaminationType> <ExaminationType>");
                return;
            }

            try {
                examinationOne = ExaminationType.valueOf(availableExaminationTypes[0].toUpperCase());
                examinationTwo = ExaminationType.valueOf(availableExaminationTypes[1].toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid examination type. Valid types are: KNEE, ELBOW, HIP");
            }

        }
    }

    private void declareQueues() throws IOException {
        exQueueOne = channel.queueDeclare(
                "technicians." + examinationOne.toString().toLowerCase(),
                false,
                false,
                false,
                null
        ).getQueue();
        exQueueTwo = channel.queueDeclare(
                "technicians." + examinationTwo.toString().toLowerCase(),
                false,
                false,
                false,
                null
        ).getQueue();
    }

    private void bindQueues() throws IOException {
        channel.queueBind(exQueueOne, EXAMINATION_EXCHANGE, "exam:" + examinationOne.toString().toLowerCase());
        channel.queueBind(exQueueTwo, EXAMINATION_EXCHANGE, "exam:" + examinationTwo.toString().toLowerCase());

    }



    private  void handleExamination(String message) throws InterruptedException, IOException {
        Random rand = new Random();
        String[] messageParts = message.split(" ");

        System.out.println("[Doctor " + messageParts[0] + "] " + " please examine patient named: " + messageParts[1]
                + ". Examination type: " + messageParts[2]);
        Thread.sleep(rand.nextInt(1, 10) * 1000L);


        String replyMessage = messageParts[1] + " " + messageParts[2];
        System.out.println("[Technician | " + messageParts[2] +"]" + " patient examined: " + messageParts[1]);
        // TODO autoAck off

        channel.basicPublish(RESULTS_EXCHANGE, "result:" + messageParts[0], null, replyMessage.getBytes(StandardCharsets.UTF_8));
    }

    private DeliverCallback setExaminationHandler() {

        return (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
                handleExamination(message);
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }

        };

    }

    private void listenQueues(DeliverCallback deliverCallback) throws IOException {
        // nasłuchuj na 2 kolejkach związanych z tym, czego leczysz
        channel.basicConsume(exQueueOne, true, deliverCallback, consumerTag -> {
        });
        channel.basicConsume(exQueueTwo, true, deliverCallback, consumerTag -> {
        });
    }


    public void run() throws Exception {
        setAvailableExaminationTypes();
        declareQueues();
        bindQueues();

        DeliverCallback deliverCallback = setExaminationHandler();
        listenQueues(deliverCallback);

    }


    public static void main(String[] args) throws Exception {
        Technician technician = new Technician();
        technician.run();
    }


}
