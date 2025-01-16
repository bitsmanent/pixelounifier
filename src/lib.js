/*
https://sqlfiddle.com/postgresql/online-compiler?id=02050823-81b4-4fb5-8d8f-afb7827af40c
*/

/*
const matchStatus = {
	ACTIVE: 1,
	DISABLED: 2
};

const oddStatus = {
	ACTIVE: 1,
	DISABLED: 2
};
*/

export const entityTypes = {
	GROUP: 1,
	CATE: 2,
	MANI: 3,
	EVENT: 4,
	PARTECIPANT: 5,
	MARKET: 6,
	OUTCOME: 7
};

export async function getMappedId(type, source, name) {
	switch(type) {
	case entityTypes.GROUP:
		/*
		 * exists source,name in source_groups
		 * true: return source_groups.group_id;
		 * false:
		 *   - insert name into groups
		 *   - insert name,source,group_id into source_groups
		*/
		break;
	default:
		return console.warn("%s: unknown entity (%s, %s)", type, source, name);
	}
}

/*
export async function publishEntity(source, type, data) {
	const user = process.env.RMQUSER;
	const pass = process.env.RMQPASS;
	const token = Buffer.from(`${user}:${pass}`).toString("base64");
	const payload = {type,data,source,ts:getISO8601()};

	const cfg = {
		method: "POST",
		headers: {
			"Content-Type": "application/json;charset=UTF-8",
			Authorization: `Basic ${token}`
		},
		body: JSON.stringify({
			properties: {
				headers: {
					MessageType: type,
					correlation_id: getId()
				},
			},
			routing_key: rmqConfig.routing_key,
			payload: JSON.stringify(payload),
			payload_encoding: "string"
		})
	};

	const [json, err] = await http(rmqConfig.publish_uri, cfg);
	if(err)
		return err;
	const resData = JSON.parse(json);
	if(resData.error)
		return resData.reason;
	return null;
}
*/
