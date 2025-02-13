import {getClient} from "./db.js";
import {outcomeStatus} from "./lib.js";

async function upsert(table, items, insertFields, conflictFields, changeFields) {
	const client = await getClient();
	const csvFields = insertFields.join(',');
	const sqlValues = [];
	const values = [];
	const updatedAtValSql = "(NOW() at time zone 'utc')";
	let valIndex = 0;

	items.forEach(item => {
		const curValues = [];

		insertFields.forEach(fld => {
			curValues.push(`$${++valIndex}`);
			values.push(item[fld]);
		});
		curValues.push(updatedAtValSql);

		sqlValues.push(`(${curValues.join(',')})`);
	});

	const conflictSet = changeFields.map(x => `${x} = EXCLUDED.${x}`).join(',');
	const updChecks = changeFields.map(x => `${table}.${x} IS DISTINCT FROM EXCLUDED.${x}`).join(" OR ");

	const sql = `
	INSERT INTO ${table} (${csvFields},updated_at)
	VALUES ${sqlValues.join(',')}
	ON CONFLICT (${conflictFields.join(',')}) DO UPDATE
	SET ${conflictSet}
	,updated_at = ${updatedAtValSql}
	,changed = CASE
		WHEN ${updChecks} THEN TRUE
		ELSE ${table}.changed
	END
	-- WHERE ${updChecks}
	`;

	//console.log(sql);

	const [res, err] = await client.exec(sql, values);
	if(err)
		console.log("Error in upsert(): %s\nSQL: %s", err, sql);
	client.release();
	return [res, err];
}

export async function handleGroups(extGroups, ctx) {
	if(!extGroups.length)
		return console.warn("handleGroups() called with empty set.");
	const items = extGroups.map(x => ({
		source: ctx.source,
		name: x.name,
		external_id: x.id
	}));
	await upsert("source_groups", items, ["source", "name", "external_id"],
		["source", "external_id"], ["name"]);
}

export async function handleCates({groupId: extGroupId,cates: extCates}, ctx) {
	if(!extCates.length)
		return console.warn("handleCates() called with empty set.");
	const items = extCates.map(x => ({
		source: ctx.source,
		name: x.name,
		external_id: x.id,
		external_group_id: extGroupId
	}));

	await upsert("source_categories", items, ["source", "name", "external_id", "external_group_id"],
		["source", "external_id", "external_group_id"], ["name"]);
}

export async function handleManis(ext, ctx) {
	if(!ext.manis.length)
		return console.warn("handleManis() called with empty set.");
	const items = ext.manis.map(x => ({
		source: ctx.source,
		name: x.name,
		external_id: x.id,
		external_category_id: ext.cateId,
		external_group_id: ext.groupId
	}));

	await upsert("source_manifestations", items, ["source", "name", "external_id", "external_category_id", "external_group_id"],
		["source", "external_id", "external_category_id", "external_group_id"], ["name"]);
}

export async function handleClasses(ext, ctx) {
	if(!ext.classes.length)
		return console.warn("handleClasses() called with empty set.");
	const items = ext.classes.map(x => ({
		source: ctx.source,
		name: x.name,
		external_id: x.id,
		external_group_id: ext.groupId
	}));

	await upsert("source_markets", items, ["source", "name", "external_id", "external_group_id"],
		["source", "external_id", "external_group_id"], ["name"]);
}

export async function handleEvents(ext, ctx) {
	if(!ext.events.length)
		return console.warn("handleEvents() called with empty set.");
	/* XXX start date */
	const items = ext.events.map(x => ({
		source: ctx.source,
		name: x.name,
		external_id: x.id,
		external_manifestation_id: ext.maniId
	}));

	const homeTeams = ext.events.map(x => ({
		source: ctx.source,
		team_name: "home",
		name: x.homeTeam,
		external_id: x.homeTeamId,
		external_event_id: x.id
	}));
	const awayTeams = ext.events.map(x => ({
		source: ctx.source,
		team_name: "away",
		name: x.awayTeam,
		external_id: x.awayTeamId,
		external_event_id: x.id
	}));

	/* XXX start date */
	await upsert("source_events", items, ["source", "name", "external_id", "external_manifestation_id"],
		["source", "external_id", "external_manifestation_id"], ["name"]);
	await upsert("source_participants", [...homeTeams, ...awayTeams],
		["source", "team_name", "name", "external_id", "external_event_id"],
		["source", "external_id", "external_event_id"], ["name"]);
}

export async function handleGames(extGames, ctx) {
	if(!extGames.length)
		return console.warn("handleGames() called with empty set.");
	const items = extGames.map(x => ({
		source: ctx.source,
		name: x.outcomeName,
		value: x.odd,
		state: x.enabled ? outcomeStatus.ACTIVE : outcomeStatus.DISABLED,
		external_id: x.outcomeId,
		external_market_id: x.marketId,
		external_event_id: x.eventId
	}));

	await upsert("source_outcomes", items,
		["source", "name", "value", "state", "external_id", "external_market_id", "external_event_id"],
		["source", "external_id", "external_market_id", "external_event_id"], ["name", "value", "state"]);
}
