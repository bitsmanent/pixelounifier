import {getClient} from "./db.js";
import {cleanString,groupBy} from "./lib.js";

async function getSourceGroups() {
	const client = await getClient();
	const [res, err] = await client.exec(`
	UPDATE source_groups
	SET changed = FALSE
	WHERE changed = TRUE AND group_id IS NULL
	RETURNING id,source,name
	`);

	if(err) {
		console.log("Groups error: %s", err);
		client.release();
		return [];
	}
	client.release();
	return res.rows;
}

async function handleSourceGroups(sourceGroups) {
	const client = await getClient();
	const groupedGroups = groupBy(sourceGroups, x => cleanString(x.name));

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
	}
	client.release();
}

async function getSourceCategories() {
	const client = await getClient();
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
		console.log("Categories error: %s", err);
		client.release();
		return [];
	}
	client.release();
	return res.rows;
}

async function handleSourceCategories(sourceCategories) {
	const client = await getClient();
	const groupedCategories = groupBy(sourceCategories, x => cleanString(x.name));

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
	}
	client.release();
}

async function getSourceManifestations() {
	const client = await getClient();
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
		console.log("Manifestations error: %s", err);
		client.release();
		return [];
	}
	client.release();
	return res.rows;
}

async function handleSourceManifestations(sourceManifestations) {
	const client = await getClient();
	const groupedManifestations = groupBy(sourceManifestations, x => cleanString(x.name));

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
	}
	client.release();
}

async function getSourceEvents() {
	const client = await getClient();
	const [res, err] = await client.exec(`
	UPDATE source_events se
	SET changed = FALSE
	FROM source_manifestations sm
	WHERE se.changed = TRUE AND sm.external_id = se.external_manifestation_id
	RETURNING se.id,se.source,se.name,se.date,sm.manifestation_id,se.event_id
	,(select start_time from events e where e.id = se.event_id) as start_time
	`);

	if(err) {
		console.log("Events error: %s", err);
		client.release();
		return [];
	}
	client.release();
	return res.rows;
}

async function handleSourceEvents(sourceEvents) {
	const client = await getClient();
	const groupedEvents = groupBy(sourceEvents, x => x.event_id || cleanString(x.name));
	let res, err;

	for(const key in groupedEvents) {
		const events = groupedEvents[key];
		const sourceEventIds = events.map(x => x.id);
		const eventName = events[0].name;
		const eventDate = events[0].date;
		const manifestationId = events[0].manifestation_id;
		let eventId = events[0].event_id;

		if(!eventId) {
			[res, err] = await client.exec(`
			INSERT INTO events (name,start_time,manifestation_id) VALUES ($1,$2,$3) RETURNING id
			`, [eventName, eventDate, manifestationId]);
			if(err) {
				console.log("INSERT INTO events: %s", err);
				continue;
			}
			eventId = res.rows[0].id;

			[res, err] = await client.exec(`
			UPDATE source_events
			SET event_id = $1
			WHERE id = ANY($2)
			`, [eventId, sourceEventIds]);
			if(err) {
				console.log("UPDATE source_events: %s", err);
				continue;
			}
		}

		const isDateChanged = events.some(x => x.start_time.getTime() != eventDate.getTime());

		if(isDateChanged) {
			/* TODO: handle data change in one or more sources */
		}
	}
	client.release();
}

export async function processDataSources() {
	const sourceGroups = await getSourceGroups();
	const sourceCategories = await getSourceCategories();
	const sourceManifestations = await getSourceManifestations();
	const sourceEvents = await getSourceEvents();

	await handleSourceGroups(sourceGroups);
	await handleSourceCategories(sourceCategories);
	await handleSourceManifestations(sourceManifestations);
	await handleSourceEvents(sourceEvents);
}
