/*
 * TODO:
 *
 * - replace loop { queries, handle } with bulkQueries,loop { handle }
 * - caching
 * - improve strings matching (ignore accents, punctuation, etc.)
 * - handle enabled flag for games
 * - split handlers into ad-hoc files
 */

import {getClient,insertMany,updateMany} from "./db.js";
import {entityStatus,outcomeStatus,sendUpdates} from "./lib.js";

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

async function processEventOutcomes(eventOutcomes) {
	const client = await getClient();
	const updates = [];
	let res, err;

	const tuples = eventOutcomes.map(x => [x.event_id, x.market_id, x.outcome_id]);
	const values = tuples.map((_, i) => `($${i * 3 + 1}::integer, $${i * 3 + 2}::integer, $${i * 3 + 3}::integer)`).join(',');

	/* Note: we must JOIN here to get the outcome name. This can be avoid
	 * by sending names each time we get a new outcome. Also for markets. */
	[res, err] = await client.exec(`
		SELECT so.event_id,so.market_id,so.outcome_id,so.value,oc.name as name
		FROM source_outcomes so
		JOIN outcomes oc ON oc.id = so.outcome_id
		WHERE (so.event_id, so.market_id, so.outcome_id) = ANY(ARRAY[${values}])
	`, tuples.flat());
	if(err) {
		console.log("Error: %s", err);
		client.release();
		return;
	}
	const sourceOutcomes = res.rows;

	eventOutcomes.forEach(eventOutcome => {
		const fltr = (a,b) =>
			a.event_id == b.event_id
			&& a.market_id == b.market_id
			&& a.outcome_id == b.outcome_id;
		const eventSourceOutcomes = sourceOutcomes.filter(x => fltr(x, eventOutcome));
		const avg = eventSourceOutcomes.reduce((acc, item) => acc + item.value, 0) / (eventSourceOutcomes.length || 1);

		const outcomeState = outcomeStatus.ACTIVE;

		/* TODO: should not send updates if avg is the same */
		updates.push({
			type: "game",
			state: eventOutcome.state,
			data: {
				state: outcomeState,
				event_id: eventOutcome.event_id,
				market_id: eventOutcome.market_id,
				outcome_id: eventOutcome.outcome_id,
				name: eventSourceOutcomes[0].name,
				value: avg
			}
		});
	});

	client.release();
	handleUpdates(updates);
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
		res.rows.forEach(n => names.push({...n,isNew:true}));
	}

	client.release();
	return names;
}

export async function handleGroups(groups, ctx) {
	const client = await getClient();
	const groupIds = groups.map(x => x.id);
	const updates = [];
	let res, err, cur;

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

				updates.push({
					type: "group",
					state: entityStatus.CREATED,
					data: {
						id: groupId,
						name: group.name
					}
				});
			}

			[res, err] = await client.exec(`
				INSERT INTO source_groups (source,name,group_id,external_id)
				VALUES ($1, $2, $3, $4)
			`, [ctx.source, group.name, groupId, group.id]);
			if(err) {
				console.log("Error: %s", err);
				continue;
			}
		} else if(sourceGroup.name.toLowerCase() != group.name.toLowerCase()) {
			console.log("UPDATE source_groups", group);

			/*
			updates.push({
				type: "group",
				state: entityStatus.UPDATED,
				data: {
					id: groupId,
					name: group.name
				}
			});
			*/
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
			RETURNING c.id,c.name
		`, [groupId, group.id, ctx.source]);
		if(err) {
			console.log("Error: %s", err);
			continue;
		}
		res.rows.forEach(row => {
			updates.push({
				type: "category",
				state: entityStatus.CREATED,
				data: {
					id: row.id,
					name: row.name,
					groupId: groupId
				}
			});
		});
	}
	client.release();
	handleUpdates(updates);
}

export async function handleCates({groupId: extGroupId,cates}, ctx) {
	const client = await getClient();
	const cateIds = cates.map(x => x.id);
	const updates = [];
	let res, err, groupId;

	[res, err] = await client.exec(`
		SELECT group_id
		FROM source_groups
		WHERE source = $1 AND external_id = $2
	`, [ctx.source, extGroupId]);
	if(err) {
		console.log("Error: %s", err);
		client.release();
		return;
	}
	if(res.rows.length)
		groupId = res.rows[0].group_id;

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
			let row, gid;

			[res, err] = await client.exec(`
				SELECT id
				FROM categories
				WHERE LOWER(name) = $1`, [lowerCateName]);
			if(err) {
				console.log("Error: %s", err);
				continue;
			}

			cateId = res.rows.length ? res.rows[0].id : null;
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
			if(err) {
				console.log("Error: %s", err);
				continue;
			}

			/* do not send partial categories (wait for groupId first) */
			if(groupId) {
				updates.push({
					type: "category",
					state: entityStatus.CREATED,
					data: {
						id: cateId,
						name: cate.name,
						groupId: groupId
					}
				});
			}

		}
		else if(sourceCate.name.toLowerCase() != cate.name.toLowerCase()) {
			console.log("UPDATE source_categories", cate);

			/*
			updates.push({
				type: "category",
				state: entityStatus.UPDATED,
				data: {
					id: cateId,
					name: cate.name,
					groupId: groupId
				}
			});
			*/
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
			RETURNING m.id,m.name
		`, [cateId, cate.id, ctx.source]);
		if(err) {
			console.log("Error: %s", err);
			continue;
		}
		res.rows.forEach(row => {
			updates.push({
				type: "manifestation",
				state: entityStatus.CREATED,
				data: {
					id: row.id,
					name: row.name,
					categoryId: cateId
				}
			});
		});
	}
	client.release();
	handleUpdates(updates);
}

export async function handleManis({cateId: extCateId,manis}, ctx) {
	const client = await getClient();
	const maniIds = manis.map(x => x.id);
	const updates = [];
	let cateId, res, err;

	[res, err] = await client.exec(`
		SELECT category_id
		FROM source_categories
		WHERE source = $1 AND external_id = $2
	`, [ctx.source, extCateId]);
	if(err) {
		console.log("Error: %s", err);
		client.release();
		return;
	}

	if(res.rows.length)
		cateId = res.rows[0].category_id;

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
			[res, err] = await client.exec(`
				SELECT id,category_id
				FROM manifestations
				WHERE LOWER(name) = $1`, [lowerManiName]);
			if(err) {
				console.log("Error: %s", err);
				continue;
			}
			maniId = res.rows.length ? res.rows[0].id : null;

			if(!maniId) {
				[res, err] = await client.exec(`
					INSERT INTO manifestations (name,category_id)
					VALUES ($1, $2)
					RETURNING id
				`, [mani.name, cateId]);

				if(err) {
					console.log("Error: %s", err);
					continue;
				}
				maniId = res.rows[0].id;

				updates.push({
					type: "manifestation",
					state: entityStatus.CREATED,
					data: {
						id: maniId,
						name: mani.name,
						categoryId: cateId
					}
				});
			}

			[res, err] = await client.exec(`
				INSERT INTO source_manifestations (source,name,manifestation_id,external_id,external_category_id)
				VALUES ($1, $2, $3, $4, $5)
			`, [ctx.source, mani.name, maniId, mani.id, extCateId]);
			if(err)
				console.log("Error: %s", err);

		} else if(sourceMani.name.toLowerCase() != mani.name.toLowerCase()) {
			console.log("UPDATE source_manifestations", mani);
			/*
			updates.push({
				type: "manifestation",
				state: entityStatus.UPDATED,
				data: {
					id: maniId,
					name: mani.name,
					categoryId: cateId
				}
			});
			*/

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
			RETURNING e.id,e.start_time
		`, [maniId, mani.id, ctx.source]);
		if(err) {
			console.log("Error: %s", err);
			continue;
		}
		res.rows.forEach(row => {
			updates.push({
				type: "event",
				state: entityStatus.CREATED,
				data: {
					id: row.id,
					startTime: row.start_time
				}
			});
		});
	}
	client.release();
	handleUpdates(updates);
}

export async function handleClasses({classes:extMarkets}, ctx) {
	const client = await getClient();
	const extMarketIds = [...new Set(extMarkets.map(x => x.id))];
	const updates = [];
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
		const market = markets.find(x => x.lowerName == lowerName);

		if(!market) {
			/* This should never happens since we already called ensureNames() */
			console.log("Cannot find marketId for %s", extMarket.name);
			continue;
		}


		if(!sourceMarket) {
			newSourceMarkets.push({
				source: ctx.source,
				name: extMarket.name,
				market_id: market.id,
				external_id: extMarket.id
			});
			updates.push({
				type: "market",
				state: entityStatus.CREATED,
				data: {
					id: market.id,
					name: market.name
				}
			});
			continue;
		}

		/* TODO update market if needed... */
	}

	if(newSourceMarkets.length) {
		[res, err] = await insertMany("source_markets",
			["source", "name", "market_id", "external_id"],
			newSourceMarkets);
		if(err)
			console.log("Error: %s", err);
	}

	client.release();
	handleUpdates(updates);
	finalizeSourceOutcomes(ctx.source, "market_id");
}

async function finalizeSourceOutcomes(source, column) {
	const client = await getClient();
	let sql, res, err;

	switch(column) {
	case "event_id":
		sql = `
		UPDATE source_outcomes so SET event_id = se.event_id
		FROM source_events se
		WHERE se.external_id = so.external_event_id
		AND se.source = so.source
		AND so.event_id IS NULL
		AND se.source = $1
		`;
		break;
	case "market_id":
		sql = `
		UPDATE source_outcomes so SET market_id = sm.market_id
		FROM source_markets sm
		WHERE sm.external_id = so.external_market_id
		AND sm.source = so.source
		AND so.market_id IS NULL
		AND sm.source = $1
		`;
	}

	/* Note: RETURNING DISTINCT thrown a syntax error so we get rid of
	 * duplicates later... */
	sql += `RETURNING so.event_id,so.market_id,so.outcome_id,so.value`;

	[res, err] = await client.exec(sql, [source]);
	if(err)
		return console.log("Error: %s", err);

	const finalizedSourceOutcomes = res.rows.filter(x => x.market_id && x.event_id);
	const newEventOutcomes = [];

	const duplicated = {};
	for(const sourceOutcome of finalizedSourceOutcomes) {
		const k = [sourceOutcome.event_id, sourceOutcome.market_id, sourceOutcome.outcome_id].join('.');

		if(duplicated[k])
			continue;
		duplicated[k] = 1;

		newEventOutcomes.push({
			event_id: sourceOutcome.event_id,
			market_id: sourceOutcome.market_id,
			outcome_id: sourceOutcome.outcome_id,
			value: 0
		});
	}
	if(newEventOutcomes.length) {
		[res, err] = await insertMany("event_outcomes",
			["event_id", "market_id", "outcome_id", "value"],
			newEventOutcomes, null);
		if(err)
			console.log("Error: %s", err);
		processEventOutcomes(newEventOutcomes.map(x => ({...x,state:entityStatus.CREATED})));
	}
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
		SELECT id,outcome_id,external_id,value
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
	const newEventOutcomes = [];
	const updSourceOutcomes = [];
	let eventToProcess = [];

	outcomes.forEach(x => x.lowerName = x.name.toLowerCase()); /* for convenience */

	for(const extOutcome of extOutcomes) {
		const sourceOutcome = sourceOutcomes.find(x => x.external_id == extOutcome.outcomeId);
		const sourceMarket = sourceMarkets.find(x => x.external_id == extOutcome.marketId);
		const sourceEvent = sourceEvents.find(x => x.external_id == extOutcome.eventId);
		const lowerName = extOutcome.outcomeName.toLowerCase();
		const outcome = outcomes.find(x => x.lowerName == lowerName);

		if(!outcome) {
			/* This should never happens since we already called ensureNames() */
			console.log("Cannot find outcome for %s", extOutcome.outcomeName);
			continue;
		}

		if(!sourceOutcome) {
			const alreadyInserting = newSourceOutcomes.some(x => x.external_id == extOutcome.outcomeId
				&& x.external_market_id == extOutcome.marketId
				&& x.external_event_id == extOutcome.eventId);

			if(!alreadyInserting) {
				if(isNaN(parseInt(extOutcome.odd)))
					debugger;
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
			if(sourceOutcome.value != extOutcome.odd) {
				updSourceOutcomes.push({
					id: sourceOutcome.id,
					value: extOutcome.odd
				});
			}
		}

		/* if fails then we got outcomes before the event or before the
		 * market which is an out-of-order flow handled elsewhere like
		 * in handleEvents() and handleMarkets() */
		if(sourceEvent && sourceMarket) {
			const eventOutcome = eventOutcomes.some(x => x.event_id == sourceEvent.event_id
				&& x.market_id == sourceMarket.market_id
				&& x.outcome_id == outcome.id);

			if(!eventOutcome) {
				const alreadyInserting = newEventOutcomes.some(x => x.event_id == sourceEvent.event_id
					&& x.market_id == sourceMarket.market_id
					&& x.outcome_id == outcome.id);

				if(alreadyInserting)
					continue;
				newEventOutcomes.push({
					event_id: sourceEvent.event_id,
					market_id: sourceMarket.market_id,
					outcome_id: outcome.id,
					value: 0 /* computed in processEventOutcomes() */
				});
			} else if(sourceOutcome) {
				if(sourceOutcome.value != extOutcome.odd) {
					eventToProcess.push({
						event_id: sourceEvent.event_id,
						market_id: sourceMarket.market_id,
						outcome_id: outcome.id,
						state: entityStatus.UPDATED
					});
				}
			}
		}
	}

	if(newSourceOutcomes.length) {
		[res, err] = await insertMany("source_outcomes", [
			"source", "name", "value",
			"outcome_id", "market_id", "event_id",
			"external_id", "external_market_id", "external_event_id"
		], newSourceOutcomes);
		if(err)
			console.log("Error: %s", err);
	}

	if(updSourceOutcomes.length) {
		[res, err] = await updateMany("source_outcomes", ["value::integer"], updSourceOutcomes, "id");
		if(err)
			console.log("Error: %s", err);

	}

	if(newEventOutcomes.length) {
		[res, err] = await insertMany("event_outcomes", [
			"event_id", "market_id", "outcome_id", "value"],
			newEventOutcomes, null);
		if(err)
			console.log("Error: %s", err);
		processEventOutcomes(newEventOutcomes.map(x => ({...x,state:entityStatus.CREATED})));
		eventToProcess = [...eventToProcess, ...newEventOutcomes.map(x => ({...x,state:entityStatus.CREATED}))];
	}

	if(eventToProcess.length)
		processEventOutcomes(eventToProcess);
	client.release();
}

export async function handleEvents({maniId:extManiId,events:extEvents}, ctx) {
	const client = await getClient();
	const extPartNames = [...new Set(extEvents.map(x => ([x.homeTeam, x.awayTeam])).flat())];
	const participants = await ensureNames("participants", extPartNames);
	const newSourceParticipants = [];
	const updates = [];
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

			updates.push({
				type: "event",
				state: entityStatus.CREATED,
				data: {
					id: eventId,
					startTime: extEvent.date
				}
			});

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
			/* TODO: Update event (start_time, etc.) or participants if needed... */
		}

		let sourceParticipant;

		sourceParticipant = sourceParticipants.find(x => x.participant_id == home.id);
		if(!sourceParticipant) {
			newSourceParticipants.push({
				source: ctx.source,
				participant_id: home.id,
				name: home.name,
				external_id: extEvent.homeTeamId /* Note: may be 0 (e.g. for unibet) */
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
				external_id: extEvent.awayTeamId /* Note: may be 0 (e.g. for unibet) */
			});
		} else {
			/* TODO: update participant (name...?) */
		}
	}

	if(newSourceParticipants.length) {
		/*
		const uniqNSP = newSourceParticipants.reduce((acc,p) => {
			if(!acc.found[p.participant_id]) {
				acc.found[p.participant_id] = 1;
				acc.uniq.push(p);
			}
			return acc;

		}, {uniq:[],found:{}}).uniq;

		// TODO: ensure this actually happens
		if(uniqNSP.length != newSourceParticipants.length)
			debugger;
		*/

		[res, err] = await insertMany("source_participants",
			["source", "participant_id", "name", "external_id"],
			newSourceParticipants, null);
		if(err)
			console.log("Error: %s", err);
	}

	client.release();
	handleUpdates(updates);
	finalizeSourceOutcomes(ctx.source, "event_id");
}
