/*
 * TODO
 *
 * - Add team_name into event_participants?
 * - Use bulk updates/insert whenever possible.
 */

import {getClient,insertMany,updateMany,upsert} from "./db.js";
import {cleanString,entityStatus,entityTypes,groupBy,matchStatus,outcomeStatus} from "./lib.js";

let client; /* shared between processors */

async function getSourceGroups() {
	const [res, err] = await client.exec(`
	UPDATE source_groups
	SET changed = FALSE
	WHERE changed = TRUE AND group_id IS NULL
	RETURNING id,source,name
	`);

	if(err) {
		console.log("getSourceGroups(): %s", err);
		return [];
	}
	return res.rows;
}

async function processSourceGroups() {
	const sourceGroups = await getSourceGroups();
	const groupedGroups = groupBy(sourceGroups, x => cleanString(x.name));
	const updates = [];

	for(const group in groupedGroups) {
		const groups = groupedGroups[group];
		const sourceGroupIds = groups.map(x => x.id);
		const groupName = groups[0].name;
		let res, err;

		[res, err] = await client.exec(`
		INSERT INTO groups (name) VALUES ($1) RETURNING id
		`, [groupName]);
		if(err) {
			console.log("INSERT INTO groups: %s", err);
			continue;
		}
		const groupId = res.rows[0].id;

		[res, err] = await client.exec(`
		UPDATE source_groups
		SET group_id = $1
		WHERE id = ANY($2)
		`, [groupId, sourceGroupIds]);
		if(err) {
			console.log("UPDATE source_groups: %s", err);
			continue;
		}

		updates.push({
			type: "group",
			state: entityStatus.CREATED,
			data: {
				id: groupId,
				name: groupName
			}
		});
	}

	return updates;
}

async function getSourceCategories() {
	const [res, err] = await client.exec(`
	UPDATE source_categories sc
	SET changed = FALSE
	FROM source_groups sg
	WHERE sc.changed = TRUE
	AND sc.category_id IS NULL AND sg.group_id IS NOT NULL
	AND sg.external_id = sc.external_group_id
	RETURNING sc.id,sc.source,sc.name,sg.group_id
	`);

	if(err) {
		console.log("getSourceManifestations(): %s", err);
		return [];
	}
	return res.rows;
}

async function processSourceCategories() {
	const sourceCategories = await getSourceCategories();
	const groupedCategories = groupBy(sourceCategories, x => cleanString(x.name));
	const updates = [];

	for(const category in groupedCategories) {
		const categories = groupedCategories[category];
		const sourceCategoryIds = categories.map(x => x.id);
		const categoryName = categories[0].name;
		const groupId = categories[0].group_id;
		let res, err;

		[res, err] = await client.exec(`
		INSERT INTO categories (name,group_id) VALUES ($1,$2) RETURNING id
		`, [categoryName, groupId]);
		if(err) {
			console.log("INSERT INTO categories: %s", err);
			continue;
		}
		const categoryId = res.rows[0].id;

		[res, err] = await client.exec(`
		UPDATE source_categories
		SET category_id = $1
		WHERE id = ANY($2)
		`, [categoryId, sourceCategoryIds]);
		if(err) {
			console.log("UPDATE source_categories: %s", err);
			continue;
		}

		updates.push({
			type: "category",
			state: entityStatus.CREATED,
			data: {
				id: categoryId,
				name: categoryName,
				groupId
			}
		});
	}

	return updates;
}

async function getSourceManifestations() {
	const [res, err] = await client.exec(`
	UPDATE source_manifestations sm
	SET changed = FALSE
	FROM source_categories sc
	WHERE sm.changed = TRUE
	AND sm.manifestation_id IS NULL AND sc.category_id IS NOT NULL
	AND sc.external_id = sm.external_category_id
	RETURNING sm.id,sm.source,sm.name,sc.category_id
	`);

	if(err) {
		console.log("getSourceManifestations(): %s", err);
		return [];
	}
	return res.rows;
}

async function processSourceManifestations() {
	const sourceManifestations = await getSourceManifestations();
	const groupedManifestations = groupBy(sourceManifestations, x => cleanString(x.name));
	const updates = [];

	for(const manifestation in groupedManifestations) {
		const manifestations = groupedManifestations[manifestation];
		const sourceManifestationIds = manifestations.map(x => x.id);
		const manifestationName = manifestations[0].name;
		const categoryId = manifestations[0].category_id;
		let res, err;

		[res, err] = await client.exec(`
		INSERT INTO manifestations (name,category_id) VALUES ($1,$2) RETURNING id
		`, [manifestationName, categoryId]);
		if(err) {
			console.log("INSERT INTO manifestations: %s", err);
			continue;
		}
		const manifestationId = res.rows[0].id;

		[res, err] = await client.exec(`
		UPDATE source_manifestations
		SET manifestation_id = $1
		WHERE id = ANY($2)
		`, [manifestationId, sourceManifestationIds]);
		if(err) {
			console.log("UPDATE source_manifestations: %s", err);
			continue;
		}

		updates.push({
			type: "manifestation",
			state: entityStatus.CREATED,
			data: {
				id: manifestationId,
				name: manifestationName,
				categoryId
			}
		});
	}

	return updates;
}

async function getSourceEvents() {
	const [res, err] = await client.exec(`
	UPDATE source_events se
	SET changed = FALSE
	FROM source_manifestations sm
	WHERE se.changed = TRUE AND sm.external_id = se.external_manifestation_id
	RETURNING se.id,se.source,se.name,se.date,sm.manifestation_id,se.event_id
	,(select start_time from events e where e.id = se.event_id) as start_time
	`);

	if(err) {
		console.log("getSourceEvents(): %s", err);
		return [];
	}
	return res.rows;
}

async function processSourceEvents() {
	const sourceEvents = await getSourceEvents();
	const groupedEvents = groupBy(sourceEvents, x => x.event_id || cleanString(x.name));
	const newEventIds = [];
	const updates = [];
	let eventParticipantsUpdates = [];
	let res, err;

	for(const key in groupedEvents) {
		const events = groupedEvents[key];
		const sourceEventIds = events.map(x => x.id);
		const eventName = events[0].name;
		const eventDate = events[0].date;
		const manifestationId = events[0].manifestation_id;
		let eventId = events[0].event_id;

		if(!eventId) {
			/*
			[res, err] = await client.exec(`
			SELECT id FROM events
			WHERE name = $1
			`, [eventName]);
			if(err)
				console.log("processSourceEvents(): %s", err);
			else if(res.rows.length)
				eventId = res.rows[0].id;
			*/

			if(!eventId) {
				[res, err] = await client.exec(`
				INSERT INTO events (name,start_time,manifestation_id) VALUES ($1,$2,$3) RETURNING id
				`, [eventName, eventDate, manifestationId]);
				if(err) {
					console.log("processSourceEvents(): %s", err);
					continue;
				}
				eventId = res.rows[0].id;
				newEventIds.push(eventId);
				updates.push({
					type: "event",
					state: entityStatus.CREATED,
					data: {
						state: matchStatus.ACTIVE, /* XXX are we sure it's active? */
						id: eventId,
						startTime: eventDate, /* XXX to ISO? */

						/* filled later */
						groupName: null,
						categoryName: null,
						manifestationName: null,
						homeTeam: null,
						awayTeam: null
					}
				});
			}
		}

		const toMapIds = [];
		sourceEventIds.forEach(id => {
			const m = events.find(x => x.id == id);

			if(!m.event_id)
				toMapIds.push(id);
		});

		if(toMapIds) {
			[res, err] = await client.exec(`
			UPDATE source_events
			SET event_id = $1
			WHERE id = ANY($2)
			`, [eventId, sourceEventIds]);
			if(err) {
				console.log("processSourceEvents(): %s", err);
				continue;
			}
		}

		const isDateChanged = events.some(x => x.start_time && x.start_time.getTime() != eventDate.getTime());

		if(isDateChanged) {
			/* TODO: handle data change in one or more sources */
		}
	}

	/* We can assume events and participants always comes together.
	 * In fact handleEvents() receives the events along with all the
	 * participants. Thus it's safe to process participants here. */
	if(newEventIds.length)
		eventParticipantsUpdates = await processEventsParticipants(newEventIds);

	const eventUpdates = updates.filter(x => x.type == "event" && x.state == entityStatus.CREATED);

	if(eventUpdates.length) {
		[res, err] = await client.exec(`
		SELECT e.id as event_id
		,p.name as participantname
		,m.name as manifestationname
		,c.name as categoryname
		,g.name as groupname
		FROM events e
		JOIN event_participants ep ON ep.event_id = e.id
		JOIN participants p on p.id = ep.participant_id
		JOIN manifestations m ON m.id = e.manifestation_id
		JOIN categories c ON c.id = m.category_id
		JOIN groups g ON g.id = c.group_id
		WHERE e.id = ANY($1)
		`, [eventUpdates.map(x => x.event_id)]);
		if(err)
			console.log("processSourceEvents(): %s", err);
		const updateInfos = err ? [] : res.rows;

		if(updateInfos.length) {
			eventUpdates.forEach(upd => {
				const info = updateInfos.filter(x => x.event_id == upd.event_id);

				Object.assign(upd, {
					groupName: info.groupname,
					categoryName: info.categoryname,
					manifestationName: info.manifestationname,
					homeTeam: info.find(x => x.team_name == "home"),
					awayTeam: info.find(x => x.team_name == "away")
				});
			});
		}
	}

	return [];
	//return [...updates, eventParticipantsUpdates];
}

async function processEventsParticipants(eventIds) {
	const updates = [];
	let res, err;

	[res, err] = await client.exec(`
	UPDATE source_participants sp
	SET changed = FALSE
	FROM source_events se
	WHERE sp.changed = TRUE
	AND se.event_id = ANY($1)
	AND se.external_id = sp.external_event_id
	AND sp.participant_id IS NULL -- just to be sure
	RETURNING sp.id,sp.name,se.event_id
	`, [eventIds]);

	if(err) {
		console.log("processEventsParticipants(): %s", err);
		return;
	}

	const sourceParticipants = res.rows;
	if(!sourceParticipants.length)
		return;

	let names = [];

	sourceParticipants.forEach(sp => {
		const key = cleanString(sp.name);

		if(!names[key])
			names[key] = sp.name;
	});
	names = Object.values(names);

	[res, err] = await client.exec(`
	SELECT name,id
	FROM participants
	WHERE name = ANY($1)
	`, [names]);
	if(err)
		return console.log("processEventsParticipants(): %s", err);
	const existingParticipants = res.rows;

	let newParticipants = [];

	/* filter out existing names */
	names = names.filter(n => !existingParticipants.some(x => x.name == n));

	if(names.length) {
		[res, err] = await insertMany("participants", ["name"],
			names.map(name => ({name})), "id", client);
		if(err)
			return console.log("processEventsParticipants(): %s", err);
		newParticipants = res.rows;
	}

	const participants = [...existingParticipants, ...newParticipants];

	participants.forEach(p => p.key = cleanString(p.name));

	const newEventParticipants = sourceParticipants.map(sp => ({
		event_id: sp.event_id,
		participant_id: participants.find(p => p.key == cleanString(sp.name)).id
	}));

	[res, err] = await insertMany("event_participants",
			["event_id", "participant_id"], newEventParticipants,
			null, client);
	if(err)
		console.log("processEventsParticipants(): %s", err);

	const updSourceParticipants = sourceParticipants.map(sp => ({
		id: sp.id,
		participant_id: participants.find(p => p.key == cleanString(sp.name)).id
	}));
	[res, err] = await updateMany("source_participants", ["participant_id::integer"], updSourceParticipants);
	if(err)
		console.log("processEventsParticipants(): %s", err);
	return updates;
}

async function getSourceMarkets() {
	const [res, err] = await client.exec(`
	UPDATE source_markets sma
	SET changed = FALSE
	FROM source_groups sg
	WHERE sma.changed = TRUE
	AND sg.external_id = sma.external_group_id
	AND sg.group_id IS NOT NULL
	RETURNING sma.id,sma.source,sma.name,sma.market_id
	`);

	if(err) {
		console.log("getSourceMarkets(): %s", err);
		return [];
	}
	return res.rows;
}

async function processSourceMarkets() {
	const sourceMarkets = await getSourceMarkets();
	const groupedMarkets = groupBy(sourceMarkets, x => x.market_id || cleanString(x.name));
	const updates = [];

	for(const market in groupedMarkets) {
		const markets = groupedMarkets[market];
		const sourceMarketIds = markets.map(x => x.id);
		const marketName = markets[0].name;
		let marketId = markets[0].market_id;
		let res, err;

		if(!marketId) {
			[res, err] = await client.exec(`
			SELECT id FROM markets
			WHERE name = $1
			`, [marketName]);
			if(err)
				console.log("SELECT FROM markets: %s", err);
			else if(res.rows.length)
				marketId = res.rows[0].id;

			if(!marketId) {
				[res, err] = await client.exec(`
				INSERT INTO markets (name) VALUES ($1) RETURNING id
				`, [marketName]);
				if(err) {
					console.log("INSERT INTO markets: %s", err);
					continue;
				}
				marketId = res.rows[0].id;
			}
		}

		const toMapIds = [];
		sourceMarketIds.forEach(id => {
			const m = markets.find(x => x.id == id);

			if(!m.market_id)
				toMapIds.push(id);
		});

		if(toMapIds) {
			[res, err] = await client.exec(`
			UPDATE source_markets
			SET market_id = $1
			WHERE id = ANY($2)
			`, [marketId, toMapIds]);
			if(err) {
				console.log("UPDATE source_markets: %s", err);
				continue;
			}
		}
	}
	return updates;
}

async function getSourceOutcomes() {
	const [res, err] = await client.exec(`
	UPDATE source_outcomes so
	SET changed = FALSE
	FROM source_markets sm
	JOIN source_events se ON se.event_id IS NOT NULL
	WHERE so.changed = TRUE
	-- AND ( so.outcome_id IS NULL OR so.market_id IS NULL OR so.event_id IS NULL)
	AND se.external_id = so.external_event_id
	AND sm.external_id = so.external_market_id
	AND sm.market_id IS NOT NULL
	RETURNING so.id,so.source,so.name,so.value,so.updated_at
	,so.outcome_id,so.market_id,so.event_id
	,sm.market_id as real_market_id,se.event_id as real_event_id
	`);
	if(err) {
		console.log("getSourceOutcomes(): %s", err);
		return [];
	}
	return res.rows;
}

async function processSourceOutcomes() {
	const sourceOutcomes = await getSourceOutcomes();

	if(!sourceOutcomes.length)
		return [];

	const partialOutcomes = sourceOutcomes.filter(x => !x.outcome_id || !x.market_id || !x.event_id);
	const outcomeNames = [...new Set(partialOutcomes.filter(x => !x.outcome_id).map(x => x.name))];
	const updSourceOutcomes = [];
	const updates = [];
	let fullOutcomesUpdates = [];
	let outcomes = [], res, err;

	if(outcomeNames.length) {
		[res, err] = await client.exec(`
		SELECT id,name
		FROM outcomes
		WHERE name = ANY($1)
		`, [outcomeNames]);
		if(err)
			console.log("handleSourceOutcomes(): %s", err);
		else
			outcomes = res.rows;

		[res, err] = await upsert("outcomes", outcomeNames.map(x => ({name:x})), ["name"], ["name"], ["name"], false);
		if(err) {
			console.log("handleSourceOutcomes(): %s", err);
			return;
		}
	}

	partialOutcomes.forEach(so => {
		const upd = {
			id: so.id,
			outcome_id: so.outcome_id || outcomes.find(x => x.name == so.name).id,
			market_id: so.market_id || so.real_market_id,
			event_id: so.event_id || so.real_event_id
		};

		Object.assign(so, upd);
		updSourceOutcomes.push(upd);
	});

	if(updSourceOutcomes.length) {
		[res, err] = await updateMany("source_outcomes",
			["outcome_id::integer", "market_id::integer", "event_id::integer"], updSourceOutcomes);
		if(err)
			console.log("handleSourceOutcomes(): %s", err);
	}

	const fullOutcomes = sourceOutcomes.filter(x => x.outcome_id && x.market_id && x.event_id);

	if(fullOutcomes.length)
		fullOutcomesUpdates = await processFullOutcomes(fullOutcomes);
	return [...updates, ...fullOutcomesUpdates];
}

async function processFullOutcomes(fullOutcomes) {
	const tuples = fullOutcomes.map(x => [x.event_id, x.market_id, x.outcome_id]);
	const values = tuples.map((_, i) => `($${i * 3 + 1}::integer, $${i * 3 + 2}::integer, $${i * 3 + 3}::integer)`).join(',');
	const updates = [];
	let res, err;

	[res, err] = await client.exec(`
	SELECT event_id,market_id,outcome_id,state,value
	FROM event_outcomes
	WHERE (event_id, market_id, outcome_id) = ANY(ARRAY[${values}])
	`, tuples.flat());
	if(err)
		console.log("handleSourceOutcomes(): %s", err);
	const eventOutcomes = res.rows;

	const groupedOutcomes = groupBy(fullOutcomes, x => [x.event_id, x.market_id, x.outcome_id].join('.'));
	const newEventOutcomes = [];
	const updEventOutcomes = [];
	for(const key in groupedOutcomes) {
		const groupOutcomes = groupedOutcomes[key];
		const value = Math.round(groupOutcomes.reduce((acc, item) => acc + item.value, 0) / (groupOutcomes.length || 1));
		const state = groupOutcomes.every(x => x.state == outcomeStatus.ACTIVE) ? outcomeStatus.ACTIVE : outcomeStatus.DISABLED;
		const {event_id,market_id,outcome_id} = groupOutcomes[0];
		const processedOutcome = {event_id,market_id,outcome_id,value,state};

		/* TODO: if updated_at is not the same for all for
		 * event/market/outcome, then it has been removed. */

		const isNew = !eventOutcomes.some(x => x.event_id == event_id
			&& x.market_id == market_id
			&& x.outcome_id == outcome_id);
		if(isNew)
			newEventOutcomes.push(processedOutcome);
		else
			updEventOutcomes.push(processedOutcome);
	}

	if(newEventOutcomes.length) {
		[res, err] = await insertMany("event_outcomes",
			["event_id", "market_id", "outcome_id", "value", "state"],
			newEventOutcomes, null);
		if(err)
			console.log("processFullOutcomes(): %s", err);
	}

	if(updEventOutcomes.length) {
		[res, err] = await updateMany("event_outcomes", ["value::integer", "state::integer"],
			updEventOutcomes, ["event_id", "market_id", "outcome_id"]);
		if(err)
			console.log("processFullOutcomes(): %s", err);
	}
	return updates;
}

function sendUpdates(updates) {
	console.log("Updates", updates);
}

export async function processDataSources() {
	client = await getClient();
	const updates = [
		...await processSourceGroups(),
		...await processSourceCategories(),
		...await processSourceManifestations(),
		...await processSourceEvents(),
		...await processSourceMarkets(),
		...await processSourceOutcomes()
	];
	client.release();
	sendUpdates(updates);
}
