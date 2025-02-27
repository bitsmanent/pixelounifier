import pg from "pg";
const {Pool} = pg;

const pool = new Pool();

pool.on("error", (err,_client) => {
	console.error("Unexpected error on idle client: %s", err);
	process.exit(-1);
});

export async function getClient() {
	const client = await pool.connect();
	const query = client.query;
	//const release = client.release;

	client.exec = async (...args) => {
		try {
			return [await query.apply(client, args)];
		} catch(e) {
			return ["", e];
		}
	};

	/*
	client.query = (...args) => {
		return query.apply(client, args);
	};

	client.release = () => {
		return release.apply(client);
	};
	*/

	return client;
}

function buildInsertMany(table, fields, items, primaryKey = "id") {
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
	/* XXX sqlValues.join(',')? */
	const sql = `
		INSERT INTO ${table} (${csvFields})
		VALUES ${sqlValues}
		${returning}
	`;

	return [sql, values];
}

export async function insertMany(table, fields, items, primaryKey = "id", reuseClient) {
	const client = reuseClient || await getClient();
	const [sql, values] = buildInsertMany(table, fields, items, primaryKey);
	const [res, err] = await client.exec(sql, values);

	if(!reuseClient)
		client.release();
	return [res, err];
}

export async function updateMany(table, fields, items, primaryKeyOrKeys = "id") {
	const client = await getClient();
	const tempTable = "temp_table";
	const untypedFields = fields.map(x => x.split(':')[0]);
	const primaryKeys = typeof primaryKeyOrKeys == "string" ? [primaryKeyOrKeys] : primaryKeyOrKeys;
	const tempFields = [
		...primaryKeys.map(x => `${x} INT`),
		...untypedFields.map(x => `${x} TEXT`),
	].join(',');
	const setFields = untypedFields.map((x,i) => `${x} = tmp.${fields[i]}`).join(',');
	const whereClause = primaryKeys.map(x => `t.${x} = tmp.${x}`).join(" AND ");
	let res, err;

	try {
		[res, err] = await client.exec("BEGIN");
		if(err) throw err;

		[res, err] = await client.exec(`CREATE TEMP TABLE ${tempTable} (${tempFields}) ON COMMIT DROP`);
		if(err) throw err;

		[res, err] = await insertMany(tempTable, [...primaryKeys, ...untypedFields], items, null, client);
		if(err) throw err;

		[res, err] = await client.exec(`
			UPDATE ${table} AS t
			SET ${setFields}
			FROM ${tempTable} AS tmp
			WHERE ${whereClause}
		`);
		if(err) throw err;

		await client.query("COMMIT");
	} catch(e) {
		await client.query("ROLLBACK");
		res = null;
		err = e;
	}

	client.release();
	return [res, err];
}

export async function upsert(table, items, insertFields, conflictFields, changeFields, trackChanges = true) {
	const client = await getClient();
	const csvFields = insertFields.join(',');
	const updatedAtValSql = "(NOW() at time zone 'utc')";
	const sqlValues = [];
	const values = [];
	let valIndex = 0;

	if(trackChanges) {
		const updChecks = changeFields.map(x => `${table}.${x} IS DISTINCT FROM EXCLUDED.${x}`).join(" OR ");

		csvFields.push("updated_at");
		changeFields.push(`updated_at = ${updatedAtValSql}`);
		changeFields.push(`changed = CASE WHEN ${updChecks} THEN TRUE ELSE ${table}.changed END`);
	}


	items.forEach(item => {
		const curValues = [];

		insertFields.forEach(fld => {
			curValues.push(`$${++valIndex}`);
			values.push(item[fld]);
		});
		if(trackChanges)
			curValues.push(updatedAtValSql);
		sqlValues.push(`(${curValues.join(',')})`);
	});

	const conflictSet = changeFields.map(x => `${x} = EXCLUDED.${x}`).join(',');
	const sql = `
	INSERT INTO ${table} (${csvFields})
	VALUES ${sqlValues.join(',')}
	ON CONFLICT (${conflictFields.join(',')}) DO UPDATE
	SET ${conflictSet}
	`;

	//console.log(sql);

	const [res, err] = await client.exec(sql, values);
	if(err)
		console.log("Error in upsert(): %s\nSQL: %s", err, sql);
	client.release();
	return [res, err];
}
