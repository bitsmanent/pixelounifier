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
		await chan.assertQueue(rmqConfig.queue, {durable: true});
	} catch(e) {
		return handler({type:"error",data:e.message});
	}

	const processed = {};
	chan.consume(rmqConfig.queue, msg => {
		const txt = msg.content.toString();
		const content = JSON.parse(txt);

		chan.ack(msg);
		if(processed[txt]) {
			console.log("Skip message of type %s from %s...", content.type, content.source);
			if(processed.tm)
				clearTimeout(processed.tm);
			processed.tm = setTimeout(() => {
				processed.tm = 0;
				console.log("Clearing duplicate cache after 200ms...");
			}, 200);
			return;
		}
		processed[txt] = 1;


		handler(content);
	});
}

async function handleMessage(type, source, data) {
	return new Promise(resolve => {
		enqueueMessage(type, source, data, resolve);
	});
}

async function enqueueMessage(type, source, data, resolve) {
	//const keyLock = "sequential";
	const keyLock = type;

	if(queueLocks[keyLock]) {
		if(!waitingQueue[keyLock])
			waitingQueue[keyLock] = [];
		waitingQueue[keyLock].push({type,source,data,resolve});
		console.log("Queued message of type %s from %s (%s/%s)...",
			type,
			source,
			waitingQueue[keyLock].length,
			Object.values(waitingQueue).reduce((sum,q) => sum += q.length, 0)
		);
		return;
	}
	queueLocks[keyLock] = 1;

	const handler = messageHandlers[type];
	const ctx = {type,source};

	if(!handler) {
		delete queueLocks[keyLock];
		return resolve(null);
	}

	const r = await handler(data, ctx);

	resolve(r);
	delete queueLocks[keyLock];

	const queued = waitingQueue[keyLock]?.shift();

	if(queued) {
		console.log("Dequeued a message of type %s from %s (%s/%s)...",
			queued.type,
			queued.source,
			waitingQueue[keyLock].length,
			Object.values(waitingQueue).reduce((sum,q) => sum += q.length, 0)
		);

		await enqueueMessage(queued.type, queued.source, queued.data, queued.resolve);
	}
}

async function main() {
	process.on("uncaughtException", console.error);
	onMessage(async ({type,source,data}) => {
		if(type == "error")
			return console.error(data);
		console.log("Handle message of type %s from %s", type, source);
		await handleMessage(type, source, data);
	});
}

main();
