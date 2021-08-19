import type { NextApiRequest, NextApiResponse } from 'next'
import db from '../../db'

export default function handler(req: NextApiRequest, res: NextApiResponse<any>) {
	const searchQuery = req.query.search
	if (Array.isArray(searchQuery) || !searchQuery) {
		res.status(400).json({ error: 'bad search query' })
		return
	}
	const stmt = db.prepare('select id, description from drgs where description like ? limit 5;')
	res.status(200).json(stmt.all(`%${searchQuery}%`))
}
