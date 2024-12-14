curl http://localhost:1234/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {
        "role": "system",
        "content": "You are a helpful assistant."
      },
      {
        "role": "user", 
        "content": "Hello, how are you?"
      }
    ],
    "temperature": 0.7,
    "max_tokens": 500
  }'
