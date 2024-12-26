import amqp, { Channel, Connection } from "amqplib"
import dotenv from "dotenv"
import { User } from "../models/User"

dotenv.config()

let channel: Channel
let connection: Connection

const requestQueue = "USER_DETAILS_REQUEST";
const responseQueue = "USER_DETAILS_RESPONSE";

const RabbitMQService = async () => {
    try {
        connection = await amqp.connect(process.env.MSG_BROKER_URL as string)
        channel = await connection.createChannel()

        // Asserting queues ensures they exist
        await channel.assertQueue(requestQueue);
        await channel.assertQueue(responseQueue);

        // listenForRequests()
        console.log("RabbitMQ Connected");
    } catch (err) {
        console.error("Error initializing RabbitMQ:", err);
    }
}

/**
 * Listen for incoming messages on the request queue
 */
const listenForRequests = () => {
    channel.consume(requestQueue, async (msg) => {
        if (msg && msg.content) {
            try {
                const { userId } = JSON.parse(msg.content.toString());
                const userDetails = await getUserDetails(userId);

                // Send the user details response
                channel.sendToQueue(
                    responseQueue,
                    Buffer.from(JSON.stringify(userDetails)),
                    { correlationId: msg.properties.correlationId }
                );

                console.log("Processed request and sent response for userId:", userId);

                // Acknowledge the processed message
                channel.ack(msg);
            } catch (error) {
                console.error("Error processing message:", error);
                channel.nack(msg, false, false); // Optionally, reject the message
            }
        }
    });
};

/**
 * Fetch user details from the database
 * @param userId - User ID to fetch
 */
const getUserDetails = async (userId: string) => {
    const userDetails = await User.findById({ _id: userId }).select("-password");
    if (!userDetails) {
        throw new Error("User not found");
    }

    return userDetails;
};

export { RabbitMQService, channel, listenForRequests }