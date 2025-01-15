/*
https://sqlfiddle.com/postgresql/online-compiler?id=0747dbcc-088b-4c82-a6cc-eb2fe5d940aa
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

