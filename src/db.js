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
