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
			//console.log("Skip duplicate message of type %s from %s...", content.type, content.source);
			if(processed.tm)
				clearTimeout(processed.tm);
			processed.tm = setTimeout(() => {
				processed.tm = 0;
				//console.log("Clearing duplicate cache after 200ms...");
			}, 200);
			return;
		}
		processed[txt] = 1;


		handler(content);
	});
}

async function handleMessage(type, source, data) {
	const handler = messageHandlers[type];
	const ctx = {type,source};

	await handler(data, ctx);
}

import {processDataSources} from "./unifier.js";
async function main() {
	console.log("Running at %s...", new Date());
	process.on("uncaughtException", console.error);
	return await processDataSources();
	onMessage(async ({type,source,data}) => {
		if(type == "error")
			return console.error("onMessage() error", data);
		//console.log("Handle message of type %s from %s", type, source);
		await handleMessage(type, source, data);
	});
}

main();
