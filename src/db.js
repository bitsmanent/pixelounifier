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

export async function insertMany(table, fields, items, primaryKey = "id") {
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
	if(err)
		debugger;

	client.release();
	return [res, err];
}
