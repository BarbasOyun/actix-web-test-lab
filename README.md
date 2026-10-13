# actix-web TestLab

-Testing Stuff<br>
-Following [getting started](https://actix.rs/docs/getting-started/)<br>

## Security / Production

-.env<br>
-cert.pem + key.pem<br>

## Docker

-Build Image :<br>
docker build -t actix-testlab .<br>

-Run Container :<br>
docker run --name actix-testlab-api actix-testlab<br>

-Run App :<br>
docker compose up -d --remove-orphans<br>

## Test API

### Main Routes :

-Send String :<br>
curl -k -X POST https://127.0.0.1:8080/echo \ -H "Content-Type: text/plain" \ -d "Hello world!"<br>

-Url parameters :<br> 
https://127.0.0.1:8080/query?username=test<br>

-Send JSON :<br>
curl -k -X POST https://127.0.0.1:8080/submit -H "Content-Type: application/json" --data-raw "{\"username\":\"test user\"}"<br>

### DB Routes :

-Send temperature JSON -> Store in DB<br>
curl -k -X POST https://127.0.0.1:8080/temperature -H "Content-Type: application/json" --data-raw "{\"date_time\":\"2026-10-10T22:58:58Z\",\"temperature\":18.5}"<br>

-Get Latest Temperature :<br>
https://localhost:8080/temperature<br>
