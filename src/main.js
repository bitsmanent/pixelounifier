import dotenvx from "@dotenvx/dotenvx";
import amqp from "amqplib";

import {handleGroups} from "./handlers.js";

dotenvx.config();

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
		const content = JSON.parse(txt);

		handler(content);
	}, {noAck:false} /* preserve message */);
}

const typeLocks = {};
const waitingQueue = {};

async function handleMessage(type, source, data) {
	return new Promise(resolve => {
		enqueueMessage(type, source, data, resolve);
	});
}

async function enqueueMessage(type, source, data, resolve) {
	if(typeLocks[type]) {
		console.log("Type %s is being processed, queuing...", type);
		if(!waitingQueue[type])
			waitingQueue[type] = [];
		waitingQueue[type].push({type,source,data,resolve});
		return;
	}
	typeLocks[type] = 1;

	const handler = messageHandlers[type];
	const ctx = {type,source};

	if(!handler) {
		delete typeLocks[type];
		resolve(null);
		return; // console.log("%s: unhandled message", type, data);
	}

	const r = await handler(data, ctx);

	resolve(r);

	delete typeLocks[type];

	const queued = waitingQueue[type]?.shift();
	if(queued)
		await enqueueMessage(queued.type, queued.source, queued.data, queued.resolve);
}

async function main() {
	process.on("uncaughtException", console.error);
	onMessage(async ({type,source,data}) => {
		await handleMessage(type, source, data);

		//console.log("handled", type, source, data);
	});
}

main();
