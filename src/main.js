import dotenvx from "@dotenvx/dotenvx";
import {processDataSources} from "./unifier.js";

dotenvx.config();

async function handleUpdates(updates) {
	if(!updates.length)
		return;

	const groups = updates.reduce((acc, upd) => {
		const groupName = upd.type+"_list";
		if(!acc[groupName])
			acc[groupName] = [];
		acc[groupName].push(upd);
		return acc;
	}, {});

	Object.keys(groups).forEach(groupName => {
		sendUpdates(groupName, groups[groupName]);
	});
}

async function performAllWorkOnce() {
	//console.log("Starting processing at %s", new Date());
	const updates = await processDataSources();
	//console.log("Finish processing at %s", new Date());

	if(!updates.length)
		return; // console.log("No updates.");
	//console.log("Sending updates...");
	handleUpdates(updates);
	console.log("Sent %d update(s).", updates.length);
}

async function run() {
	await performAllWorkOnce();
	const delay = 1000;

	//console.log("Waiting %sms...", delay);
	setTimeout(run, delay);
}

function main() {
	console.log("Running at %s...", new Date());
	process.on("uncaughtException", console.error);
	run();
}

main();
