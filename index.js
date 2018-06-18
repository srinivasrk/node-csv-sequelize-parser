const createChannelsSites = require('./api/createChannelsSites');

createChannelsSites.createSitesChannels()
.then(() => {
  createChannelsSites.updateChannels()
}).then(() => {
  console.log("FINISHED CHANNELS PROMISE")
}).catch((err) => {
  console.log(err)
})
