import amqp from "amqplib";
import strftime from "strftime";

export const entityTypes = {
	GROUP: 1,
	CATE: 2,
	MANI: 3,
	EVENT: 4,
	PARTECIPANT: 5,
	MARKET: 6,
	OUTCOME: 7
};

export const entityStatus = {
	CREATED: 1,
	UPDATED: 2,
	REMOVED: 3
};

const matchStatus = {
	ACTIVE: 1,
	DISABLED: 2
};

export const outcomeStatus = {
	ACTIVE: 1,
	DISABLED: 2
};

const getIdData = {
	lastNow: 0,
	lastIndex: 0
};

export function getId() {
	const now = Date.now();

	if(now > getIdData.lastNow + getIdData.lastIndex) {
		getIdData.lastNow = now;
		getIdData.lastIndex = 0;
	}
	return now + ++getIdData.lastIndex;
}

export function getISO8601(date = new Date()) {
	return strftime("%FT%T", date);
}

const rmqUpdates = {}; /* used to keep conn/chan opened. To be cleaned up! */

export async function sendUpdates(type, data) {
	const rmqConfig = {
		host: "rabbittest.pixelo.it",
		port: 5672,
		vhost: "Grabber",
		queue: "grabber",
	};
	const user = process.env.RMQUSER;
	const pass = process.env.RMQPASS;
	let conn, chan;

	conn = rmqUpdates.conn;
	chan = rmqUpdates.chan;

	if(!conn) {
		try {
			conn = await amqp.connect(`amqp://${user}:${pass}@${rmqConfig.host}:${rmqConfig.port}/${rmqConfig.vhost}`);
			chan = await conn.createChannel();
			await chan.assertQueue(rmqConfig.queue, {durable: true});
		} catch(e) {
			console.log("Error initializing RMQ: %s", e);
			return e.message;
		}

		rmqUpdates.conn = conn;
		rmqUpdates.chan = chan;
	}

	try {
		const payload = {type,data,ts:getISO8601()};
		const buffer = Buffer.from(JSON.stringify(payload));

		chan.sendToQueue(rmqConfig.queue, buffer, {
			persistent: true,
			headers: {
				MessageType: type,
				correlation_id: getId()
			}
		});
		console.log("%s updates sent for %s", data.length, type);
	} catch(e) {
		console.log("Error sending updates: %s", e);
		return e.message;
	}

	/*
	await chan.close();
	await conn.close();
	*/
}
