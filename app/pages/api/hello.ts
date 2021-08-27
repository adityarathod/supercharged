import type { NextApiRequest, NextApiResponse } from 'next'
import db from '../../db'

type Data = {
	name: string
}

export default function handler(req: NextApiRequest, res: NextApiResponse<any>) {
	db.all('select * from facilities;', (err, rows) => {
		res.status(200).json(rows)
	})
}
