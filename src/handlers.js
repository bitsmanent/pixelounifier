/*
 * TODO:
 *
 * - split handlers into ad-hoc files
 * - replace loop { queries, handle } with bulkQueries,loop { handle }
 * - merge handlers into handleMessage() if possible
 * - caching
 * - improve strings matching (ignore accents, punctuation, etc.)
 * - implement IDs for participants as well?
 */

import {getClient} from "./db.js";

async function insertMany(table, fields, items, primaryKey = "id") {
	const client = await getClient();
	const csvFields = fields.join(',');
	const sqlValues = [];
	const values = [];
	let valIndex = 0;
	let csvReturn;
	let returning = "";

	items.forEach(item => {
		const curValues = [];
		fields.forEach(fld => {
			curValues.push(`$${++valIndex}`);
			values.push(item[fld]);
		});
		sqlValues.push(`(${curValues.join(',')})`);
	});

	if(primaryKey) {
		csvReturn = [primaryKey, csvFields].join(',');
		returning = `RETURNING ${csvReturn}`;
	}
	const [res, err] = await client.exec(`
		INSERT INTO ${table} (${csvFields})
		VALUES ${sqlValues}
		${returning}
	`, values);

	client.release();
	return [res, err];
}

async function ensureNames(table, extNames) {
	const client = await getClient();
	const lowerNames = extNames.map(x => x .toLowerCase());
	let res, err;

	[res, err] = await client.exec(`
		SELECT id,name,LOWER(name) as "lowerName"
		FROM ${table}
		WHERE LOWER(name) = ANY($1)
	`, [lowerNames]);
	if(err) {
		console.log("Error: %s", err);
		client.release();
		return;
	}

	const names = res.rows;
	const newNames = [...new Set(extNames.filter((_,i) => {
		return !names.find(p => p.lowerName == lowerNames[i]);
	}))];

	if(newNames.length) {
		[res, err] = await insertMany(table, ["name"], newNames.map(name => ({name})));
		if(err)
			return console.log("Error: %s", err);
		res.rows.forEach(n => names.push(n));
	}

	client.release();
	return names;
}

export async function handleGroups(groups, ctx) {
	const client = await getClient();
	const groupIds = groups.map(x => x.id);
	let res, err;

	[res, err] = await client.exec(`
		SELECT group_id,external_id,name
		FROM source_groups
		WHERE source = $1 AND external_id = ANY($2)
	`, [ctx.source, groupIds]);

	if(err) {
		console.log("Error: %s", err);
		client.release();
		return;
	}

	const sourceGroups = res.rows;

	for(const group of groups) {
		const sourceGroup = sourceGroups.find(x => x.external_id == group.id);
		const lowerGroupName = group.name.toLowerCase();
		let groupId = sourceGroup?.group_id;

		if(!sourceGroup) {
			let row;

			[res, err] = await client.exec(`
				SELECT id
				FROM groups
				WHERE LOWER(name) = $1
			`, [lowerGroupName]);
			if(err) {
				console.log("Error: %s", err);
				continue;
			}
			row = res.rows[0] || {};

			groupId = row.id;
			if(!groupId) {
				[res, err] = await client.exec(`
					INSERT INTO groups (name)
					VALUES ($1) RETURNING id
				`, [group.name]);

				if(err) {
					console.log("Error: %s", err);
					continue;
				}
				groupId = res.rows[0].id;
			}

			[res, err] = await client.exec(`
				INSERT INTO source_groups (source,name,group_id,external_id)
				VALUES ($1, $2, $3, $4)
			`, [ctx.source, group.name, groupId, group.id]);
			if(err)
				console.log("Error: %s", err);
		} else if(sourceGroup.name.toLowerCase() != group.name.toLowerCase()) {
			console.log("UPDATE source_groups", group);
		} else {
			//console.log("DO NOTHING.");
		}

		/* update categories having NULL as group_id if any. */
		[res, err] = await client.exec(`
			UPDATE categories c SET group_id = $1
			FROM source_categories sc
			WHERE sc.category_id = c.id
			AND c.group_id IS NULL
			AND sc.external_group_id = $2
			AND sc.source = $3
		`, [groupId, group.id, ctx.source]);
	}

	client.release();
}

export async function handleCates({groupId: extGroupId,cates}, ctx) {
	const client = await getClient();
	const cateIds = cates.map(x => x.id);
	let res, err;

	[res, err] = await client.exec(`
		SELECT category_id,external_id,name
		FROM source_categories
		WHERE source = $1 AND external_id = ANY($2)
	`, [ctx.source, cateIds]);

	if(err) {
		console.log("Error: %s", err);
		client.release();
		return;
	}

	const sourceCates = res.rows;

	for(const cate of cates) {
		const sourceCate = sourceCates.find(x => x.external_id == cate.id);
		const lowerCateName = cate.name.toLowerCase();
		let cateId = sourceCate?.category_id;

		if(!sourceCate) {
			let row, groupId;

			[res, err] = await client.exec(`
				SELECT id,group_id
				FROM categories
				WHERE LOWER(name) = $1`, [lowerCateName]);
			if(err) {
				console.log("Error: %s", err);
				continue;
			}

			row = res.rows[0] || {};

			[groupId, cateId] = [row.group_id, row.id];
			if(!groupId) {
				[res, err] = await client.exec(`
					SELECT group_id
					FROM source_groups
					WHERE external_id = $1 AND source = $2
				`, [extGroupId, ctx.source]);

				if(err) {
					console.log("Error: %s", err);
					continue;
				}
				groupId = res.rows[0]?.group_id;
			}

			if(!cateId) {
				[res, err] = await client.exec(`
					INSERT INTO categories (name,group_id)
					VALUES ($1, $2)
					RETURNING id
				`, [cate.name, groupId]);

				if(err) {
					console.log("Error: %s", err);
					continue;
				}
				cateId = res.rows[0].id;
			}

			[res, err] = await client.exec(`
				INSERT INTO source_categories (source,name,category_id,external_id,external_group_id)
				VALUES ($1, $2, $3, $4, $5)
			`, [ctx.source, cate.name, cateId, cate.id, extGroupId]);
			if(err)
				console.log("Error: %s", err);

		}
		else if(sourceCate.name.toLowerCase() != cate.name.toLowerCase()) {
			console.log("UPDATE source_categories", cate);
		} else {
			//console.log("DO NOTHING.");
		}

		/* Update manifestations having NULL as category_id if any. */
		[res, err] = await client.exec(`
			UPDATE manifestations m SET category_id = $1
			FROM source_manifestations sm
			WHERE sm.manifestation_id = m.id
			AND m.category_id IS NULL
			AND sm.external_category_id = $2
			AND sm.source = $3
		`, [cateId, cate.id, ctx.source]);
	}

	client.release();
}

export async function handleManis({cateId: extCateId,manis}, ctx) {
	const client = await getClient();
	const maniIds = manis.map(x => x.id);
	let res, err;

	[res, err] = await client.exec(`
		SELECT manifestation_id,external_id,name
		FROM source_manifestations
		WHERE source = $1 AND external_id = ANY($2)
	`, [ctx.source, maniIds]);

	if(err) {
		console.log("Error: %s", err);
		client.release();
		return;
	}

	const sourceManis = res.rows;

	for(const mani of manis) {
		const sourceMani = sourceManis.find(x => x.external_id == mani.id);
		const lowerManiName = mani.name.toLowerCase();
		let maniId = sourceMani?.manifestation_id;

		if(!sourceMani) {
			let row, categoryId;

			[res, err] = await client.exec(`
				SELECT id,category_id
				FROM manifestations
				WHERE LOWER(name) = $1`, [lowerManiName]);
			if(err) {
				console.log("Error: %s", err);
				continue;
			}
			row = res.rows[0] || {};

			[categoryId, maniId] = [row.category_id, row.id];
			if(!categoryId) {
				[res, err] = await client.exec(`
					SELECT category_id
					FROM source_categories
					WHERE external_id = $1 AND source = $2
				`, [extCateId, ctx.source]);

				if(err) {
					console.log("Error: %s", err);
					continue;
				}
				categoryId = res.rows[0]?.category_id;
			}

			if(!maniId) {
				[res, err] = await client.exec(`
					INSERT INTO manifestations (name,category_id)
					VALUES ($1, $2)
					RETURNING id
				`, [mani.name, categoryId]);

				if(err) {
					console.log("Error: %s", err);
					continue;
				}
				maniId = res.rows[0].id;
			}

			[res, err] = await client.exec(`
				INSERT INTO source_manifestations (source,name,manifestation_id,external_id,external_category_id)
				VALUES ($1, $2, $3, $4, $5)
			`, [ctx.source, mani.name, maniId, mani.id, extCateId]);
			if(err)
				console.log("Error: %s", err);

		} else if(sourceMani.name.toLowerCase() != mani.name.toLowerCase()) {
			console.log("UPDATE source_manifestations", mani);
		} else {
			//console.log("DO NOTHING.");
		}

		/* update events having NULL as manifestation_id */
		[res, err] = await client.exec(`
			UPDATE events e SET manifestation_id = $1
			FROM source_events se
			WHERE se.event_id = e.id
			AND e.manifestation_id IS NULL
			AND se.external_manifestation_id = $2
			AND se.source = $3
		`, [maniId, mani.id, ctx.source]);
	}
	client.release();
}

export async function handleClasses({classes:extMarkets}, ctx) {
	const client = await getClient();
	const extMarketIds = [...new Set(extMarkets.map(x => x.id))];
	let res, err;

	[res, err] = await client.exec(`
		SELECT market_id,external_id
		FROM source_markets
		WHERE source = $1 AND external_id = ANY($2)
	`, [ctx.source, extMarketIds]);
	if(err) {
		console.log("Error: %s", err);
		client.release();
		return;
	}

	const sourceMarkets = res.rows;
	const newSourceMarkets = [];
	const extMarketNames = extMarkets.map(x => x.name);
	const markets = await ensureNames("markets", extMarketNames);

	markets.forEach(x => x.lowerName = x.name.toLowerCase()); /* for convenience */
	for(const extMarket of extMarkets) {
		const sourceMarket = sourceMarkets.find(x => x.external_id == extMarket.id);
		const lowerName = extMarket.name.toLowerCase();

		if(!sourceMarket) {
			const market = markets.find(x => x.lowerName == lowerName);

			if(!market) {
				/* This should never happens since we already called ensureNames() */
				console.log("Cannot find marketId for %s", extMarket.name);
				continue;
			}

			newSourceMarkets.push({
				source: ctx.source,
				name: extMarket.name,
				market_id: market.id,
				external_id: extMarket.id
			});
			continue;
		}

		/* TODO Update if needed... */
	}

	if(newSourceMarkets.length) {
		[res, err] = await insertMany("source_markets",
			["source", "name", "market_id", "external_id"],
			newSourceMarkets);
		if(err)
			console.log("Error: %s", err);
	}

	/* update source_outcomes having market_id NULL */
	[res, err] = await client.exec(`
		UPDATE source_outcomes so SET market_id = sm.market_id
		FROM source_markets sm
		WHERE sm.external_id = so.external_market_id
		AND sm.source = so.source
		AND so.market_id IS NULL
		AND sm.source = $1
	`, [ctx.source]);
	if(err)
		console.log("Error: %s", err);

	client.release();
}

export async function handleGames(extOutcomes, ctx) {
	const client = await getClient();
	const extOutcomeIds = [...new Set(extOutcomes.map(x => x.outcomeId))];
	const extOutcomeNames = [...new Set(extOutcomes.map(x => x.outcomeName))];
	const extMarketIds = [...new Set(extOutcomes.map(x => x.marketId))];
	const extEventIds = [...new Set(extOutcomes.map(x => x.eventId))];
	let res, err;

	extOutcomes.forEach(x => {
		extOutcomeIds.push(x.outcomeId);
		extOutcomeNames.push(x.outcomeName);
		extMarketIds.push(x.marketId);
		extEventIds.push(x.eventId);
	});

	[res, err] = await client.exec(`
		SELECT outcome_id,external_id
		FROM source_outcomes
		WHERE source = $1 AND external_id = ANY($2)
	`, [ctx.source, extOutcomeIds]);
	if(err) {
		console.log("Error: %s", err);
		client.release();
		return;
	}
	const sourceOutcomes = res.rows;

	[res, err] = await client.exec(`
		SELECT market_id,external_id
		FROM source_markets
		WHERE source = $1 AND external_id = ANY($2)
	`, [ctx.source, extMarketIds]);
	if(err) {
		console.log("Error: %s", err);
		client.release();
		return;
	}
	const sourceMarkets = res.rows;

	[res, err] = await client.exec(`
		SELECT event_id,external_id
		FROM source_events
		WHERE source = $1 AND external_id = ANY($2)
	`, [ctx.source, extEventIds]);
	if(err) {
		console.log("Error: %s", err);
		client.release();
		return;
	}
	const sourceEvents = res.rows;

	const eventIds = [...new Set(sourceEvents.map(x => x.event_id))];
	[res, err] = await client.exec(`
		SELECT event_id,market_id,outcome_id
		FROM event_outcomes
		WHERE event_id = ANY($1)
	`, [eventIds]);
	if(err) {
		console.log("Error: %s", err);
		client.release();
		return;
	}
	const eventOutcomes = res.rows;


	const outcomes = await ensureNames("outcomes", extOutcomeNames);
	const newSourceOutcomes = [];
	const eventOutcomesList = [];

	outcomes.forEach(x => x.lowerName = x.name.toLowerCase()); /* for convenience */

	for(const extOutcome of extOutcomes) {
		const sourceOutcome = sourceOutcomes.find(x => x.external_id == extOutcome.outcomeId);
		const sourceMarket = sourceMarkets.find(x => x.external_id == extOutcome.marketId);
		const sourceEvent = sourceEvents.find(x => x.external_id == extOutcome.eventId);
		const lowerName = extOutcome.outcomeName.toLowerCase();
		const outcome = outcomes.find(x => x.lowerName == lowerName);

		if(!sourceOutcome) {

			if(!outcome) {
				/* This should never happens since we already called ensureNames() */
				console.log("Cannot find outcome for %s", extOutcome.outcomeName);
				continue;
			}

			const alreadyInserting = newSourceOutcomes.some(x => x.external_id == extOutcome.outcomeId);
			if(!alreadyInserting) {
				newSourceOutcomes.push({
					source: ctx.source,
					name: extOutcome.outcomeName,
					value: extOutcome.odd,
					outcome_id: outcome.id,
					market_id: sourceMarket ? sourceMarket.market_id : null,
					event_id: sourceEvent ? sourceEvent.event_id : null,
					external_id: extOutcome.outcomeId,
					external_market_id: extOutcome.marketId,
					external_event_id: extOutcome.eventId
				});
			}
		}
		else {
			/* TODO update sourceOutcome if needed... */
		}

		/* if fails then we got outcomes before the event or before the
		 * market which is an out-of-order flow handled elsewhere like
		 * in handleEvents() and handleMarkets() */
		if(sourceEvent && sourceMarket && outcome) {
			const eventOutcome = eventOutcomes.find(x => x.event_id == sourceEvent.event_id
				&& x.market_id == sourceMarket.market_id
				&& x.outcome_id == outcome.id);

			if(!eventOutcome) {
				eventOutcomesList.push({
					event_id: sourceEvent.event_id,
					market_id: sourceMarket.market_id,
					outcome_id: outcome.id,
					value: 0
				});
			}
		}
	}

	if(newSourceOutcomes.length) {
		newSourceOutcomes.forEach(x => x.value = parseInt(x.value, 10)); /* XXX temp workaround */

		[res, err] = await insertMany("source_outcomes", [
			"source", "name", "value",
			"outcome_id", "market_id", "event_id",
			"external_id", "external_market_id", "external_event_id"
			], newSourceOutcomes);
		if(err)
			console.log("Error: %s", err);
	}

	if(eventOutcomesList.length) {
		[res, err] = await insertMany("event_outcomes", [
			"event_id", "market_id", "outcome_id", "value"
			], eventOutcomesList, null);
		if(err)
			console.log("Error: %s", err);

	}
	client.release();
}

export async function handleEvents({maniId:extManiId,events:extEvents}, ctx) {
	const client = await getClient();
	const extPartNames = [...new Set(extEvents.map(x => ([x.homeTeam, x.awayTeam])).flat())];
	const participants = await ensureNames("participants", extPartNames);
	const newSourceParticipants = [];
	let res, err;

	[res, err] = await client.exec(`
		SELECT name,participant_id,external_id
		FROM source_participants
		WHERE source = $1 AND participant_id = ANY($2)
	`, [ctx.source, participants.map(x => x.id)]);
	if(err) {
		console.log("Error: %s", err);
		client.release();
		return;
	}
	const sourceParticipants = res.rows;

	participants.forEach(p => p.lowerName = p.name.toLowerCase()); /* for convenience */
	for(const extEvent of extEvents) {
		const lowerHome = extEvent.homeTeam.toLowerCase();
		const lowerAway = extEvent.awayTeam.toLowerCase();
		const home = participants.find(x => x.lowerName == lowerHome);
		const away = participants.find(x => x.lowerName == lowerAway);

		[res, err] = await client.exec(`
			SELECT e.id
			FROM events e
			JOIN source_events se ON se.event_id = e.id
			WHERE se.source = $1 AND se.external_id = $2
		`, [ctx.source, extEvent.id]);
		if(err) {
			console.log("Error: %s", err);
			continue;
		}
		let eventId = res.rows[0]?.id;

		if(!eventId) {
			/* retrieve manifestation_id */
			[res, err] = await client.exec(`
				SELECT manifestation_id
				FROM source_manifestations
				WHERE source = $1 AND external_id = $2
			`, [ctx.source, extManiId]);
			if(err) {
				console.log("Error: %s", err);
				continue;
			}

			const maniId = res.rows[0]?.manifestation_id;

			[res, err] = await client.exec(`
				INSERT into events (manifestation_id,start_time)
				VALUES ($1, $2)
				RETURNING id
			`, [maniId, extEvent.date]);
			if(err) {
				console.log("Error: %s", err);
				continue;
			}
			eventId = res.rows[0].id;

			[res, err] = await client.exec(`
				INSERT INTO event_participants (event_id, participant_id)
				VALUES ($1, $2), ($1, $3)
			`, [eventId, home.id, away.id]);
			if(err) {
				console.log("Error: %s", err);
				continue;
			}

			[res, err] = await client.exec(`
				INSERT INTO source_events (source, event_id, external_id, external_manifestation_id, name)
				VALUES ($1, $2, $3, $4, $5)
			`, [ctx.source, eventId, extEvent.id, extManiId, extEvent.name]);
			if(err) {
				console.log("Error: %s", err);
				continue;
			}
		} else {
			/* TODO: Update event or participants if needed... */
		}

		let sourceParticipant;

		sourceParticipant = sourceParticipants.find(x => x.participant_id == home.id);
		if(!sourceParticipant) {
			newSourceParticipants.push({
				source: ctx.source,
				participant_id: home.id,
				name: home.name,
				external_id: home.id /* XXX temp workaround */
			});
		} else {
			/* TODO: update participant (name...?) */
		}

		sourceParticipant = sourceParticipants.find(x => x.participant_id == away.id);
		if(!sourceParticipant) {
			newSourceParticipants.push({
				source: ctx.source,
				participant_id: away.id,
				name: away.name,
				external_id: away.id /* XXX temp workaround */
			});
		} else {
			/* TODO: update participant (name...?) */
		}
	}

	if(newSourceParticipants.length) {
		const uniqNSP = newSourceParticipants.reduce((acc,p) => {
			if(!acc.found[p.participant_id]) {
				acc.found[p.participant_id] = 1;
				acc.uniq.push(p);
			}
			return acc;

		}, {uniq:[],found:{}}).uniq;

		/* TODO: ensure this actually happens */
		if(uniqNSP.length != newSourceParticipants.length)
			debugger;

		[res, err] = await insertMany("source_participants", [
			"source", "participant_id", "name", "external_id"
			], uniqNSP, null);
		if(err)
			console.log("Error: %s", err);
	}

	/* TODO: here we should handle source_outcomes having event_id NULL */

	/* update source_outcomes having event_id NULL */
	[res, err] = await client.exec(`
		UPDATE source_outcomes so SET event_id = se.event_id
		FROM source_events se
		WHERE se.external_id = so.external_event_id
		AND se.source = so.source
		AND so.event_id IS NULL
		AND se.source = $1
	`, [ctx.source]);
	if(err)
		console.log("Error: %s", err);

	client.release();
}
