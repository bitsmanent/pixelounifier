import {getClient} from "./db.js";

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
		const row = sourceGroups.find(x => x.external_id == group.id);
		const lowerGroupName = group.name.toLowerCase();

		if(!row) {
			let groupId;

			[res, err] = await client.exec("SELECT id from groups where LOWER(name) = $1", [lowerGroupName]);
			if(err) {
				console.log("Error: %s", err);
				continue;
			}
			groupId = res.rows[0]?.id;

			if(!res.rows.length) {
				[res, err] = await client.exec("INSERT INTO groups (name) VALUES ($1) RETURNING id", [group.name]);

				if(err) {
					console.log("Error: %s", err);
					continue;
				}
				groupId = res.rows[0].id;
			}

			[res, err] = await client.exec(`
			INSERT INTO source_groups
			(source,name,group_id,external_id)
			VALUES ($1, $2, $3, $4)`, [ctx.source, group.name, groupId, group.id]);
			if(err)
				console.log("Error: %s", err);
		} else if(row.name.toLowerCase() != group.name.toLowerCase()) {
			console.log("UPDATE source_groups", group);
		} else {
			console.log("DO NOTHING.");
		}
	}

	client.release();
}
