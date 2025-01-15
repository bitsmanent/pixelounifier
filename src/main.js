import dotenvx from "@dotenvx/dotenvx";
import amqp from "amqplib";

import {handleGroups} from "./handlers.js";

const messageHandlers  = {
	groups: handleGroups
};

async function onMessage(handler) {
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

async function handleMessage(type, source, message) {
	const handler = messageHandlers[type];

	if(!handler)
		console.log("%s: unhandled message", type, message);
	else
		message = await handler(message, {type,source});
	return message;
}

async function main() {
	dotenvx.config();
	onMessage(async message => {
		const handled = await handleMessage(message.type, message.grabberName, message);

		console.log("handled", handled);
	});
}

main();
