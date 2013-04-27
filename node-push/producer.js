var zmq = require('zmq')
  , sock = zmq.socket('push');

sock.bindSync('tcp://127.0.0.1:4777');
console.log('Producer bound to port 4777');

setInterval(function(){
  console.log('sending work');
  sock.send('{ \"tracking\": \"10\" }');
}, 500);
