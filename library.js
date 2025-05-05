'use strict';

const nconf = require.main.require('nconf');
const winston = require.main.require('winston');

const { Kafka } = require('kafkajs');

const meta = require.main.require('./src/meta');

const controllers = require('./lib/controllers');

const routeHelpers = require.main.require('./src/routes/helpers');

const plugin = {};

plugin.init = async (params) => {
	const { router /* , middleware , controllers */ } = params;

	// Settings saved in the plugin settings can be retrieved via settings methods
	const { setting1, setting2 } = await meta.settings.get('kafka');
	if (setting1) {
		console.log(setting2);
	}

	const kafka = new Kafka({
		clientId: "my-app",
		brokers: ["localhost:9092"],
	});

	plugin.kafka_producer = kafka.producer();
	await plugin.kafka_producer.connect();

	/**
	 * We create two routes for every view. One API call, and the actual route itself.
	 * Use the `setupPageRoute` helper and NodeBB will take care of everything for you.
	 *
	 * Other helpers include `setupAdminPageRoute` and `setupAPIRoute`
	 * */
	routeHelpers.setupPageRoute(router, '/kafka', [(req, res, next) => {
		winston.info(`[plugins/kafka] In middleware. This argument can be either a single middleware or an array of middlewares`);
		setImmediate(next);
	}], (req, res) => {
		winston.info(`[plugins/kafka] Navigated to ${nconf.get('relative_path')}/kafka`);
		res.render('kafka', { uid: req.uid });
	});

	routeHelpers.setupAdminPageRoute(router, '/admin/plugins/kafka', controllers.renderAdminPage);
};

/**
 * If you wish to add routes to NodeBB's RESTful API, listen to the `static:api.routes` hook.
 * Define your routes similarly to above, and allow core to handle the response via the
 * built-in helpers.formatApiResponse() method.
 *
 * In this example route, the `ensureLoggedIn` middleware is added, which means a valid login
 * session or bearer token (which you can create via ACP > Settings > API Access) needs to be
 * passed in.
 *
 * To call this example route:
 *   curl -X GET \
 * 		http://example.org/api/v3/plugins/quickstart/test \
 * 		-H "Authorization: Bearer some_valid_bearer_token"
 *
 * Will yield the following response JSON:
 * 	{
 *		"status": {
 *			"code": "ok",
 *			"message": "OK"
 *		},
 *		"response": {
 *			"foobar": "test"
 *		}
 *	}
 */
plugin.addRoutes = async ({ router, middleware, helpers }) => {
	const middlewares = [
		middleware.ensureLoggedIn,			// use this if you want only registered users to call this route
		middleware.admin.checkPrivileges,	// use this to restrict the route to administrators
	];

	routeHelpers.setupApiRoute(router, 'get', '/kafka/:param1', middlewares, (req, res) => {
		helpers.formatApiResponse(200, res, {
			foobar: req.params.param1,
		});
	});
};

plugin.addAdminNavigation = (header) => {
	header.plugins.push({
		route: '/plugins/kafka',
		icon: 'fa-tint',
		name: 'Kafka',
	});

	return header;
};

plugin.handelPosts = async function (post) {
	await plugin.kafka_producer.send({
		topic: "nodebb-posts",
		messages: [{
			key: String(post.post.pid),
			value: JSON.stringify(post)
		}]
	})
}

plugin.handelUploads = async function (data) {
	await plugin.kafka_producer.send({
		topic: "nodebb-images",
		messages: [{
			key: String(data.image.path),
			value: JSON.stringify(data)
		}]
	})
	return data;
}

module.exports = plugin;
