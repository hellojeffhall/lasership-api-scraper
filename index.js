// index.js

/* TODO 

Instead of promise.all, just set an interval where we fire off one request every second.
If the request ends in an error, increment an error counter.
If there are 10 errors, abort the script.
Keep a completion counter and an error counter.
Set up a simple http server so that we can check on the progress, which should give us the 
current error count, tracking ID to check, and completion count. (Current filesize of DB would be cool too.)
Bonus points for websockets so that we can have a realtime count going.
Bonus points for being able to adjust the interval from the websockets dashboard or abort from dashboard.
Being able to see errors from dashboard would be cool too.
Scrape dashboard could be its own thing TBH
Should be able to run this such that it will deposit the info into a database running on a digital ocean space
DB should check to make sure we don't already have an ID in the database
Or should start by finding the smallest ID that has made it into the database
Maybe inserts should be batched. We don't have a lot of RAM but at 2.2 kb per response we should be albe to get a good number of responses together before we need to write them
ACTUALLY we should have a database in BLOCK storage, and then dump it to spaces I guess...?
OR just run all on our linux box, and upload the results to spaces/ github if we really want to back up.
Fromt there, we can always extract the limited data that we need for data viz, etc.

*/

const stream = require('fs').createWriteStream("log.txt", {flags:'a'});

const log = text => {
	const ts = new Date().toISOString()
	stream.write(`${ts}|${text}\n`)
}

const closeLog = () => {
	stream.end();
}

const Database = require('better-sqlite3');
const db = new Database('lasership.db');

const createTableErrors = db.prepare(`
	CREATE TABLE IF NOT EXISTS ERRORS (
		ErrorMessage text,
		TrackingNumber text,
		id INTEGER PRIMARY KEY AUTOINCREMENT
	)`).run()

const createTableTrackings = db.prepare(`
	CREATE TABLE IF NOT EXISTS TRACKINGS (
		OrderNumber text,
		ReceivedOn text,
		UTCReceivedOn text,
		EstimatedDeliveryDate text,
		Origin_City text,
		Origin_State text,
		Origin_PostalCode text,
		Origin_Country text,
		Destination_City text,
		Destination_State text,
		Destination_PostalCode text,
		Destination_Country text,
		TrackingNumber text,
		id INTEGER PRIMARY KEY AUTOINCREMENT
	)`).run()

const createTablePieces = db.prepare(`
	CREATE TABLE IF NOT EXISTS PIECES (
		TrackingNumber text,
		Weight text,
		WeightUnit text,
		id INTEGER PRIMARY KEY AUTOINCREMENT
	)`).run()

const createTableEvents = db.prepare(`
	CREATE TABLE IF NOT EXISTS EVENTS (
		DateTime text,
		UTCDateTime text,
		City text,
		State text,
		PostalCode text,
		Country text,
		EventType text,
		EventModifier text,
		EventLabel text,
		EventShortText text,
		EventLongText text,
		Signature text,
		Signature2 text,
		Location text,
		Reason text,
		TrackingNumber text,
		id INTEGER PRIMARY KEY AUTOINCREMENT
	)`).run()

const insertTracking = trackingDataObject => {
	const result = db.prepare(`
		INSERT INTO TRACKINGS 
			(
				OrderNumber,
				ReceivedOn,
				UTCReceivedOn,
				EstimatedDeliveryDate,
				Origin_City,
				Origin_State,
				Origin_PostalCode,
				Origin_Country,
				Destination_City,
				Destination_State,
				Destination_PostalCode,
				Destination_Country,
				TrackingNumber
			)
			VALUES
			(
				$OrderNumber,
				$ReceivedOn,
				$UTCReceivedOn,
				$EstimatedDeliveryDate,
				$Origin_City,
				$Origin_State,
				$Origin_PostalCode,
				$Origin_Country,
				$Destination_City,
				$Destination_State,
				$Destination_PostalCode,
				$Destination_Country,
				$TrackingNumber
			)
	`).run(trackingDataObject)
}

const insertPiece = pieceDataObject => {
	const result = db.prepare(`
		INSERT INTO PIECES
			(
				TrackingNumber,
				Weight,
				WeightUnit
			)
			VALUES
			(
				$TrackingNumber,
				$Weight,
				$WeightUnit
			)
	`).run(pieceDataObject)
}

const insertError = errorDataObject => {
	const result = db.prepare(`
		INSERT INTO ERRORS
			(
				TrackingNumber,
				ErrorMessage
			)
			VALUES
			(
				$TrackingNumber,
				$ErrorMessage
			)
	`).run(errorDataObject)
}

const insertEvent = eventDataObject => {
	const result = db.prepare(`
		INSERT INTO EVENTS
			(
				DateTime,
				UTCDateTime,
				City,
				State,
				PostalCode,
				Country,
				EventType,
				EventModifier,
				EventLabel,
				EventShortText,
				EventLongText,
				Signature,
				Signature2,
				Location,
				Reason,
				TrackingNumber
			)
			VALUES
			(
				$DateTime,
				$UTCDateTime,
				$City,
				$State,
				$PostalCode,
				$Country,
				$EventType,
				$EventModifier,
				$EventLabel,
				$EventShortText,
				$EventLongText,
				$Signature,
				$Signature2,
				$Location,
				$Reason,
				$TrackingNumber
			)
	`).run(eventDataObject)
}

const getInitialTrackingNumber = () => {
	// Get the lowest tracking nubmers, whether that's in trackings or in errors
	const currentMinTrackings_raw = db.prepare('select min(TrackingNumber) from TRACKINGS').get()
	const currentMinErrors_raw = db.prepare('select min(TrackingNumber) from ERRORS').get()

	const currentMinTrackings = currentMinTrackings_raw['min(TrackingNumber)']
	const currentMinErrors = currentMinErrors_raw['min(TrackingNumber)']

	return (
		parseInt( currentMinTrackings.replace(/[^0-9]/g, '') ) < parseInt( currentMinErrors.replace(/[^0-9]/g, '') ) ?
		currentMinTrackings :
		currentMinErrors
	)
}

const isTrackingNumberAlreadyInDatabase = TrackingNumber => {
	const exists = db.prepare('select id from TRACKINGS where TrackingNumber = $TrackingNumber').get({TrackingNumber})
	return !!exists
}

isTrackingNumberAlreadyInDatabase( )

const getAllData = () => {
	const getAllEvents = db.prepare(`select * from events`).all()
	const getAllPieces = db.prepare(`select * from pieces`).all()
	const getAllTrackings = db.prepare(`select * from trackings`).all()
	console.log('EVENTS')
	console.log( JSON.stringify(getAllEvents,null,2) )
	console.log('PIECES')
	console.log( JSON.stringify(getAllPieces,null,2) )
	console.log('TRACKINGS')
	console.log( JSON.stringify(getAllTrackings,null,2) )
}

const http = require('http')

const maxTrackingID = 'LX25910663'
// const possibleLowest = 'LX22321770'

const initialTrackingNumber = 
	isTrackingNumberAlreadyInDatabase(maxTrackingID) ?
	`LX${ parseInt( getInitialTrackingNumber().replace(/[^0-9]/g, '') ) - 1}` :
	maxTrackingID

const numberOfItemsToGet = 1E6

const get = ({host, path}) => {
	return new Promise( (resolve, reject) => {
		http.get({ host, path, json : true, headers: { 'User-Agent': 'Crawling for fun/ personal project. Non-commerical use. jeffreyrhall@gmail.com' } }, response => {
			let body = '';
			response.on('data', chunk => {
				body += chunk
			})
			response.on('end', () => {
				resolve( body )
			})
			response.on('error', err => {
				reject(err)
			})
		})
	})
}

const getTrackingInfo = async trackingID => {
	const host = 'lasership.com'
	const path = `/track/LX${parseInt(trackingID.replace(/[^0-9]/g, ''))}/json`
	const result = await get({host, path})
	return result
}

const arrayOfTrackingNumbers = Array.from(
	'x'.repeat(numberOfItemsToGet)
).map( (x, idx) => {
	const justNumbers = initialTrackingNumber.replace(/[^0-9]/g, '')
	const newNumber = parseInt(justNumbers) - idx
	return 'LX' + newNumber
})

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms) )

const forEachSyncWithDelayAndMaxError = async (items, fn, ms, callback=()=>{}, index=0, errorCount=0) => {
	if( !items[index] ){ 
		console.log('Finished; calling callback.')
		log('Finished; calling callback.')
		callback()
		return
	}
	if(errorCount > 1){
		console.log('Aborting due to excessive errors (${errorCount})')
		log('Aborting due to excessive errors (${errorCount})')
		return
	}

	await fn(items[index]).catch( () => {
		errorCount = errorCount + 1
		log(`Error count at ${errorCount}`)
	})
	await sleep(ms)
	forEachSyncWithDelayAndMaxError(items, fn, ms, callback, index + 1, errorCount)
}

const handleRequest = (id) => {
	log(`Begin ${id}`)
	return new Promise((resolve, reject) => {
		getTrackingInfo( id )
			.then( data => {
				const parsed = JSON.parse(data)

				if(parsed.Error){
					insertError({
						TrackingNumber : id,
						ErrorMessage : parsed.ErrorMessage
					})
					log(`Error on ${id}`)
					console.log(`Error on ${id}`)
					resolve(parsed)
					return
				}

				const {
					OrderNumber,
					ReceivedOn,
					UTCReceivedOn,
					EstimatedDeliveryDate,
					Origin,
					Destination,
					Pieces,
					Events
				} = parsed

				// TRACKINGS and EVENTS need the TrackingNumber injected
				// PIECES already has the tracking number

				insertTracking({
					OrderNumber,
					ReceivedOn,
					UTCReceivedOn,
					EstimatedDeliveryDate,
					Origin_City : Origin.City,
					Origin_State : Origin.State,
					Origin_PostalCode : Origin.PostalCode,
					Origin_Country : Origin.Country,
					Destination_City : Destination.City,
					Destination_State : Destination.State,
					Destination_PostalCode : Destination.PostalCode,
					Destination_Country : Destination.Country, 
					TrackingNumber : id
				})

				Pieces.forEach( insertPiece )				
				Events.forEach( x => {
					insertEvent({...x, TrackingNumber : id})
				})

				process.stdout.write(`Inserted ${id}\n`)
				resolve(parsed)
			})
			.catch( err => {
				log(err)
				console.log(err)
				reject(err)
			})		
	})
}

forEachSyncWithDelayAndMaxError(arrayOfTrackingNumbers, handleRequest, 10)

