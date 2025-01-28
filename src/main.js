import dotenvx from "@dotenvx/dotenvx";
import amqp from "amqplib";

import {
	handleGroups,
	handleCates,
	handleManis,
	handleClasses,
	handleEvents,
	handleGames
} from "./handlers.js";

dotenvx.config();

const messageHandlers  = {
	groups: handleGroups,
	cates: handleCates,
	manis: handleManis,
	classes: handleClasses,
	events: handleEvents,
	games: handleGames
};

const queueLocks = {};
const waitingQueue = {};

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
		return handler({type:"error",data:e.message});
	}

	await chan.assertQueue(rmqConfig.queue, {durable: true});
	chan.consume(rmqConfig.queue, msg => {
		const txt = msg.content.toString();
		const content = JSON.parse(txt);

		handler(content);
		chan.ack(msg);
	});
}

async function handleMessage(type, source, data) {
	return new Promise(resolve => {
		enqueueMessage(type, source, data, resolve);
	});
}

async function enqueueMessage(type, source, data, resolve) {
	/* TODO: lock by type+data.name.toLowerCase() or somethingk like this? */
	//const keyLock = type;

	/* TODO: this slow down a lot. We should find a way to handle
	 * concurrent messages mainly for handleGames() which don't fill the
	 * source_outcomes market_id and event_id properly sometimes. */
	const keyLock = "sequential";

	if(queueLocks[keyLock]) {
		if(!waitingQueue[keyLock])
			waitingQueue[keyLock] = [];
		waitingQueue[keyLock].push({type,source,data,resolve});
		return;
	}
	queueLocks[keyLock] = 1;

	const handler = messageHandlers[type];
	const ctx = {type,source};

	if(!handler) {
		delete queueLocks[keyLock];
		resolve(null);
		return; // console.log("%s: unhandled message", type, data);
	}

	const r = await handler(data, ctx);

	resolve(r);
	delete queueLocks[keyLock];

	const queued = waitingQueue[keyLock]?.shift();
	if(queued)
		await enqueueMessage(queued.type, queued.source, queued.data, queued.resolve);
}

async function main() {
	process.on("uncaughtException", console.error);
	onMessage(async ({type,source,data}) => {
		if(type == "error")
			return console.error(data);
		await handleMessage(type, source, data);
	});
}

main();
