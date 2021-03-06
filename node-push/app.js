var express = require('express')
  , routes = require('./routes')
  , http = require('http')
  , path = require('path')
  , zmq = require('zmq')
  , zmqsocket = zmq.socket('pull');

zmqsocket.bindSync('tcp://127.0.0.1:4777');

var app = express();

app.configure(function(){
  app.set('port', process.env.PORT || 3000);
  app.set('views', __dirname + '/views');
  app.set('view engine', 'ejs');
  app.use(express.favicon());
  app.use(express.logger('dev'));
  app.use(express.bodyParser());
  app.use(express.methodOverride());
  app.use(express.cookieParser('your secret here'));
  app.use(express.session());
  app.use(app.router);
  app.use(express.static(path.join(__dirname, 'public')));
});

app.configure('development', function(){
  app.use(express.errorHandler());
});

app.get('/', routes.index);

var server = http.createServer(app);
server.listen(app.get('port'), function(){
  console.log("Express server listening on port " + app.get('port'));
});

var io = require('socket.io').listen(server);
io.sockets.on('connection', function (socket) {
  socket.emit('news', { hello: 'world' });
});

zmqsocket.on('message', function (data) {
  message = JSON.parse(data);
  io.sockets.emit('message', message);
  console.log('Message received from storm : ' + JSON.stringify(message));
});
  
