# Use the official Node.js image from the Docker Hub
FROM node:14-alpine

# Create and change to the app directory
WORKDIR /usr/src/app

# Copy the package.json and package-lock.json files
COPY package*.json ./

# Install project dependencies
RUN npm install

# Copy the rest of the application code
COPY . .

# Expose the port the app runs on
EXPOSE 3300

# Start the application
CMD ["node", "server.js"]
