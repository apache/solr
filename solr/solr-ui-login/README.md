
# Getting Started with Create React App

This project was bootstrapped with [Create React App](https://github.com/facebook/create-react-app).

## Available Scripts

The application can be started several ways. The recommended way is to start each service, `client` and `server` independently.

### `npm run start-server`

The backend Express app will start running on port `3001`. This service is a thin layer to handle communication between the front end and Solr or Zookeeper backends. Today, the service is only being built to support standalone, where there is no Zookeeper, to prevent the likelihood of it being run in a production environment, though there is still some configuration need to do so in standalone.

You can test the backend endpoints with `cURL`, just as you could target the Solr directly. 

### `npm run start-client`

The frontend Express app will start. This service purely handles the presentation layer and user actions. It does not actually communicate with the backend at all. If you are working only on styling or markup related edits, you may find running in this mode faster as it will pickup changes quickly. 

In the project directory, to start both the server and the client you can run. It works, but not as well and is not recommended yet:

### `npm start`

Runs the app in the development mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in your browser.

The page will reload when you make changes.\
You may also see any lint errors in the console.

### `npm test`

Launches the test runner in the interactive watch mode.\
See the section about [running tests](https://facebook.github.io/create-react-app/docs/running-tests) for more information.

### `npm run build`

This project is months from being production ready, however this section is here for reference and will evolve. 

Builds the app for production to the `build` folder.\
It correctly bundles React in production mode and optimizes the build for the best performance. 

The build is minified and the filenames include the hashes.\
Your app is ready to be deployed!

See the section about [deployment](https://facebook.github.io/create-react-app/docs/deployment) for more information.

## Learn More

You can learn more in the [Create React App documentation](https://facebook.github.io/create-react-app/docs/getting-started).

To learn React, check out the [React documentation](https://reactjs.org/).

### Code Splitting

This section has moved here: [https://facebook.github.io/create-react-app/docs/code-splitting](https://facebook.github.io/create-react-app/docs/code-splitting)

### Analyzing the Bundle Size

This section has moved here: [https://facebook.github.io/create-react-app/docs/analyzing-the-bundle-size](https://facebook.github.io/create-react-app/docs/analyzing-the-bundle-size)

### Making a Progressive Web App

This section has moved here: [https://facebook.github.io/create-react-app/docs/making-a-progressive-web-app](https://facebook.github.io/create-react-app/docs/making-a-progressive-web-app)

### Advanced Configuration

This section has moved here: [https://facebook.github.io/create-react-app/docs/advanced-configuration](https://facebook.github.io/create-react-app/docs/advanced-configuration)

### Deployment

This section has moved here: [https://facebook.github.io/create-react-app/docs/deployment](https://facebook.github.io/create-react-app/docs/deployment)

### `npm run build` fails to minify

This section has moved here: [https://facebook.github.io/create-react-app/docs/troubleshooting#npm-run-build-fails-to-minify](https://facebook.github.io/create-react-app/docs/troubleshooting#npm-run-build-fails-to-minify)

### Express Documentation

https://expressjs.com/
