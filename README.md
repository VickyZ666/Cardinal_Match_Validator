# Cardinal_Match_Validator

To run the code first install  all the dependencies by running:
```pip install -r .\requirement.txt```

To run the entire validator run the following command:
```python main.py```



### Logic Flow

- In main.py, the fetchdb() function will be called, which connects to the postSQL database, and uses the Query to select K Hired/Rejected candidate information, along with the job description. Then this K candidate info will be stored locally on a file called result.json by default. 
- After all K candidate info has been fetched, fetch_cv_text() will be called to parse the CV into pure txt, removing any styling such as \n, and append the pure txt into the JSON object stored in result.json. 
- Finally, request_match() will be called to make a POST request to the /match end point running locally. It will go through the array of JSON objects in result.json, and make a POST request with BODY populated by each info stored in JSON object. It will log the result of the /match endpoint in performance.txt locally, including info like overall score, candidate ID, sub category scores. 



### Adapting the code

- To make the code be more adaptive to your testing needs, there are a couple of things you can easily modify

  - The LIMIT 1 in Query in dbFetch.py, this defines how many candidates will be fetched from the postSQL database, as well as be POSTED to the /match api. Adjust the test sample size as you want

  - The hierarchy of the hiring status. With hired with 1, the query will fetch hired candidates first, you can change reject to be 1 to get rejected candidates. 

  - ``````sql
            CASE
              WHEN s.submission_type = 'hired' THEN 1
              WHEN s.submission_type = 'offer' THEN 2
              WHEN s.submission_type = 'second_interview' THEN 3
              WHEN s.submission_type = 'first_interview' THEN 4
              WHEN s.submission_type = 'recruiter_screen' THEN 5
              WHEN s.submission_type = 'applicant' THEN 6
              WHEN s.submission_type = 'submitted' THEN 7
              WHEN s.submission_type = 'lead' THEN 8
              WHEN s.submission_type = 'onhold' THEN 9
              WHEN s.submission_type = 'reject' THEN 10
              ELSE 11
            END
    ``````

  - In main.py, the code makes POST request to localhost at port 8080, if you are running on a different port, modify as needed. 

    ``````python
            resp = requests.post("http://localhost:8080/match", json=payload)
    ``````

    

