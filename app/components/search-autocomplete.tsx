import { FC, useEffect, useState } from 'react'
import { useRouter } from 'next/router'

type AutocompleteSuggestion = {
	id: number
	description: string
}

const SearchAutocomplete: FC = () => {
	const [search, setSearch] = useState<string>('')
	const [suggestions, _setSuggestions] = useState<AutocompleteSuggestion[]>([])
	const router = useRouter()
	useEffect(() => {
		const updateSuggestions = async () => {
			let suggestions: AutocompleteSuggestion[]
			if (search.trim().length === 0 || search === '') suggestions = []
			else {
				const res = await fetch(`/api/suggest?search=${search}`)
				suggestions = res.ok ? await res.json() : []
			}
			_setSuggestions(suggestions)
		}
		updateSuggestions()
	}, [search])

	const submitSearch = (suggestion: AutocompleteSuggestion) => {
		setSearch(suggestion.description.toLowerCase())
		router.push(`/treatment/${suggestion.id}`)
	}

	return (
		<div className='text-center relative pb-2 mb-2 w-full max-w-2xl'>
			<div className='absolute text-left z-10 top-6 pt-3'>
				{suggestions.map((sugg, idx) => {
					return (
						<div
							className='w-full cursor-pointer pt-2 text-gray-500 bg-white hover:text-gray-600 hover:font-semibold'
							onClick={() => submitSearch(sugg)}
							key={idx}>
							{sugg.description.toLowerCase()}
						</div>
					)
				})}
			</div>
			<div className='flex flex-row items-center justify-center'>
				<input
					type='text'
					className='border-b-2 border-gray-300 mr-1 outline-none flex-grow'
					placeholder='search for a treatment'
					tabIndex={1}
					value={search}
					onChange={e => setSearch(e.target.value)}
				/>
				{/* <button
					className='rounded-full w-8 h-8 text-lg flex items-center justify-center text-center bg-primaryblue text-white outline-none hover:filter focus:filter hover:brightness-95 focus:brightness-[80%]'
					tabIndex={2}>
					&rarr;
				</button> */}
			</div>
		</div>
	)
}

export default SearchAutocomplete
