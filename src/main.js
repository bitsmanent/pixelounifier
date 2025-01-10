import dotenvx from "@dotenvx/dotenvx";
import amqp from "amqplib";

dotenvx.config();

async function onEntity(handler) {
	const rmqConfig = {
		host: "rabbittest.pixelo.it",
		port: 5672,
		vhost: "Grabber",
		queue: "unifier"
	};
	const user = process.env.RMQUSER;
	const pass = process.env.RMQPASS;
	let conn, chan;

	try {
		conn = await amqp.connect(`amqp://${user}:${pass}@${rmqConfig.host}:${rmqConfig.port}/${rmqConfig.vhost}`);
		chan = await conn.createChannel();
	} catch(e) {
		return e.message;
	}

	await chan.assertQueue(rmqConfig.queue, {durable: true});
	chan.consume(rmqConfig.queue, msg => {
		const txt = msg.content.toString();
		const entity = JSON.parse(txt);

		handler(entity);
	}, {noAck:false} /* preverve message */);
}

async function main() {
	let totalEntities = 0;

	onEntity(entity => {
		++totalEntities;
		console.log("totalEntities: %s", totalEntities, entity);
	});
}

main();
