const db = require('../conf/db');
var _ = require('underscore');
const csvparse = require('csv-parse');
var fs = require('fs-extra');
var path = require('path');

var force = true;
let masterFile = path.join(path.resolve(__dirname),'..','FM Installations (View All Active Channels) 6-13-18 (1-20 PM).csv');

let updateSitesWithRow = (row,transaction) => {
  let active = (row.state.substring(0,2) != "11");
  return db.Maintainer.findOrCreate({where: {name:row.maintainer}, transaction})
  .then(([maintainer,created]) => {
    return db.Site.findOrCreate({where: {name:row.site}, include: [{model:db.Maintainer,as:'maintainer'}], transaction})
    .then(([site,created]) => {
      let updateObj = {maintainer,active};
      //console.log(`updating with: ${JSON.stringify(updateObj)}`);
      return site.update({active},{transaction})
      .then(()=>site.setMaintainer(maintainer,{transaction})) // must be separate updates for some reason
      .then(()=>site.save({transaction}));
    }); // Site findOrCreate
  }); // Maintainer findOrCreate
}

let updateChannelWithRow = (row,transaction) => {
  // console.log(row);

  console.log(row);
  return db.Site.findOrCreate({where: {name:row.site}, transaction, defaults:{active:false}})
  .then(([site,created]) => {
    // console.log('found site: ' + site)
    let filter = _.extend(_.omit(row,'site'),{site_id:site.id}); // i hate this line.
    return db.Channel.findOrCreate({where:filter, include:[{model:db.Site,as:'site'}], transaction})
    .then(([channel,created]) => {
      return channel.update(row,{transaction})
      .then(()=>channel.setSite(site,{transaction}))
      .then(()=>channel.save({transaction}));
    });
  });
};

module.exports.createSitesChannels = () => {
   return new Promise((resolve, reject) => {
     db.Site.sync({force})
    .then(() => db.Maintainer.sync({force}))
    .then(() => db.Channel.sync({force}))
    .then(() => {
      return db.connection.transaction().then((transaction) => {
         let modelUpdateChain = Promise.resolve();
         let line_num = 0;
         let streamOK = true;
         var parser = csvparse();
         parser.on('readable', function(){
            let record = parser.read();
            if (!record) return;
            ++line_num;
            if (line_num > 1) { // skip header row
              // open sites are anything other than "11"
              let row = _(['site', 'maintainer', 'state', 'influx_edited_data','scada_integration', 'sensor_type', 'measurement', 'location', 'number', 'scada_tag']).object(record); // zip up object
              if (row.maintainer.length == 0) {
                return;
              }

              modelUpdateChain = modelUpdateChain.then(() => updateSitesWithRow(row,transaction)); // append update-chain
            } // non-header line
         })
         parser.on('finish', () => {
         console.log('stream finish')
         // if stream not ok, then 'error' would have been emitted, and the main promise rejected.
         if (streamOK) {
              modelUpdateChain = modelUpdateChain.then(() => {
              transaction.commit();
              resolve();
            });
          }
        });
        fs.createReadStream(masterFile).pipe(parser);
        return modelUpdateChain;
       })
     })
   })
}

module.exports.updateChannels = () => {
  return new Promise((resolve,reject) => {
    db.Channel.sync({force})
    .then(() => {
      return db.connection.transaction().then((transaction) => {
        let modelUpdateChain = Promise.resolve();
        var streamOK = true;
        let line_num = 0;
        var parser = csvparse();
        parser.on('readable', function(){
          let record = parser.read();
          if (!record) return;
          ++line_num;

          if (line_num > 1) { // skip header row
            let row = _(['site', 'maintainer', 'state', 'influx_edited_data','scada_integration', 'sensor_type', 'measurement', 'location', 'number', 'scada_tag']).object(record); // zip up object
            row = _(row).mapObject((v,k) => {
              if (k != 'site') {
                return v.toLowerCase()
              }
              else {
                return v;
              }
            });
            modelUpdateChain = modelUpdateChain.then(() => updateChannelWithRow(row,transaction)); // append update-chain
          } // non-header line
        }); // on readable
        parser.on('error', (err) => {
          transaction.rollback();
          reject(err.message);
        });
        parser.on('finish', () => {
          console.log('stream finish')
          if (streamOK) {
            modelUpdateChain = modelUpdateChain.then(() => {
              transaction.commit();
              resolve();
            });
          }
          // if stream not ok, then 'error' would have been emitted, and the main promise rejected.
        });
        fs.createReadStream(masterFile).pipe(parser);
        // if streaming a file, i.e., with POST data:
        //parser.write(inStream);
        //parser.end();
        return modelUpdateChain;
      }); // transaction
    })
  });
};
