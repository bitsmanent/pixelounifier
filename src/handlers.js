import {entityTypes,getMappedId} from "./lib.js";

export async function handleGroups(message, ctx) {
	for(let i = 0, len = message.groups.length; i < len; i++) {
		const group = message.groups[i];
		group.groupId = getMappedId(entityTypes.GROUP, ctx.source, group.name);
	}
	return message;
}
