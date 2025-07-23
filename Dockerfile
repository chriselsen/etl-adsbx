FROM public.ecr.aws/lambda/nodejs:20

# Copy package files first for better layer caching
COPY package*.json ${LAMBDA_TASK_ROOT}/

# Install dependencies
RUN npm install

# Copy source files
COPY . ${LAMBDA_TASK_ROOT}/

# Build TypeScript files
RUN npm run build

# Ensure the dist directory has the correct files
RUN ls -la ${LAMBDA_TASK_ROOT}/dist/

CMD ["dist/task.handler"]
