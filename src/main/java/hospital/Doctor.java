package hospital;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static hospital.ExaminationType.*;


public class Doctor {
    private final static String EXAMINATION_EXCHANGE = "examination";
    private final static String RESULTS_EXCHANGE = "results";
    private final static String INFO_EXCHANGE = "info";
    private final static String LOG_EXCHANGE = "log";

    private final Channel channel;
    private String doctorName;


    private final Scanner sc;

    private String resultsQueue;
    private String infoQueue;

    private final UUID uuid;


    public Doctor() throws IOException, TimeoutException {
        channel = createChannel();
        declareExchanges();

        uuid = UUID.randomUUID();
        sc = new Scanner(System.in);
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
        channel.exchangeDeclare(INFO_EXCHANGE, "fanout");
        channel.exchangeDeclare(LOG_EXCHANGE, "fanout");
    }


    private void declareQueues() throws IOException {
        resultsQueue = channel.queueDeclare(
                "doctors." + doctorName,
                false,
                false,
                true,
                null
        ).getQueue();

        infoQueue = channel.queueDeclare().getQueue();

    }

    private void bindQueues() throws IOException {
        channel.queueBind(resultsQueue, RESULTS_EXCHANGE, "result:" + doctorName);
        channel.queueBind(infoQueue, INFO_EXCHANGE, "");
    }

    private String readDoctorName() throws IOException {
        System.out.print("Enter doctor name: ");
        if (sc.hasNextLine()) {
            return sc.nextLine().trim();
        } else {
            throw new IOException("Doctor name is a must");
        }
    }

    private DeliverCallback setResultsHandler() {

        return (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
                handleResults(message);
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }

        };

    }

    private DeliverCallback setInfoHandler() {

        return (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
                handleInfo(message);
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }

        };

    }


    private void listenQueues(String queueName, DeliverCallback deliverCallback) throws IOException {
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }

    private void handleResults(String message) throws InterruptedException, IOException {
        Random rand = new Random();
        String[] parts = message.split(" ");

        String logMessage = "[Technician | " + parts[1] + "]" + " patient's  examined: " + parts[0];

        System.out.println(logMessage);

        logMessage = "Doctor@" + uuid + ": " + logMessage;
        channel.basicPublish(LOG_EXCHANGE, "", null, logMessage.getBytes(StandardCharsets.UTF_8));
        // simulate heavy task
        Thread.sleep(rand.nextInt(1, 10) * 1000L);

        logMessage = "[Doctor " + doctorName + "]" + " Now I can start patient's " + parts[0] + " treatment";
        System.out.println(logMessage);

        logMessage = "Doctor@" + uuid + ": " + logMessage;
        channel.basicPublish(LOG_EXCHANGE, "", null, logMessage.getBytes(StandardCharsets.UTF_8));
    }

    private void handleInfo(String message) throws InterruptedException {
        System.out.println("[Admin] " + message);
    }

    private void run() throws IOException {
        doctorName = readDoctorName();
        declareQueues();
        bindQueues();

        DeliverCallback resultsDeliveryCallback = setResultsHandler();
        DeliverCallback infoDeliveryCallback = setInfoHandler();
        listenQueues(resultsQueue, resultsDeliveryCallback);
        listenQueues(infoQueue, infoDeliveryCallback);


        // main loop
        while (sc.hasNextLine()) {
            String examinationJob = sc.nextLine();
            String[] examinationArgs = examinationJob.split(" ");

            if (examinationArgs.length != 2) {
                System.out.println("Unknown command");
                continue;
            }

            String examinationTypeStr = examinationArgs[0];
            String patientName = examinationArgs[1];

            ExaminationType examinationType;
            try {
                examinationType = ExaminationType.valueOf(examinationTypeStr.toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid examination type: " + examinationTypeStr);
                continue;
            }

            String order = doctorName + " " + patientName + " " + examinationType.toString().toLowerCase();
            orderExamination(examinationType, channel, order);


        }

        sc.close();
    }


    private void orderExamination(ExaminationType examinationType, Channel channel, String message) throws IOException {

        String[] parts = message.split(" ");


        System.out.println("[Doctor " + doctorName + "] please examine patient named " + parts[1]
                + ". Examination type " + examinationType.toString().toLowerCase()
        );

        switch (examinationType) {
            case KNEE:
                channel.basicPublish(
                        EXAMINATION_EXCHANGE,
                        "exam:" + KNEE.toString().toLowerCase(),
                        null,
                        message.getBytes(StandardCharsets.UTF_8)
                );
                break;
            case HIP:
                channel.basicPublish(
                        EXAMINATION_EXCHANGE,
                        "exam:" + HIP.toString().toLowerCase(),
                        null,
                        message.getBytes(StandardCharsets.UTF_8)
                );
                break;
            case ELBOW:
                channel.basicPublish(
                        EXAMINATION_EXCHANGE,
                        "exam:" + ELBOW.toString().toLowerCase(),
                        null,
                        message.getBytes(StandardCharsets.UTF_8)
                );
                break;
        }
    }


    public static void main(String[] args) throws IOException, TimeoutException {
        Doctor doctor = new Doctor();
        doctor.run();
    }


}
