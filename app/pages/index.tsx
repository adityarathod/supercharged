import Head from 'next/head'
import Image from 'next/image'
import SearchAutocomplete from '../components/search-autocomplete'
import styles from '../styles/Home.module.css'

export default function Home() {
	return (
		<div>
			<Head>
				<title>supercharged: home</title>
				<meta
					name='description'
					content="supercharged ⚡️: find out if you've been overcharged for your medical treatment*"
				/>
				<link rel='icon' href='/favicon.ico' type='image/x-icon' />
			</Head>

			<main
				className='w-full h-full flex flex-col items-center justify-center p-4'
				style={{ height: '100vh' }}>
				<div className='w-full max-w-[800px] flex flex-col items-center'>
					<div className='mb-8 text-center'>
						<h1 className='font-bold text-4xl mb-2'>⚡️ supercharged</h1>
						<h2 className='text-gray-500'>
							find out if you&apos;ve been overcharged for your medical treatment*
						</h2>
					</div>
					<SearchAutocomplete />
				</div>
			</main>
		</div>
	)
}
