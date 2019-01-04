function importDocument(jsonArray) {
    let collection = getContext().getCollection(),
        collectionLink = collection.getSelfLink(),
        count = 0,
        callback = (err, document, options) => {
                            if (err) {throw error}
                            count++;

                            if (count >= jsonCount) {
                                getContext().getResponse().setBody(count);
                            } else {
                                tryCreate(jsonArray[count], callback);
                            }
                        },  
        tryCreate = (document, callback) =>{
                            let options = {
                                disableAutomaticIdGeneration: true /** documentID is not going to parse your documents, to look for an ID. This speed up your request*/
                            },
                                isAccepted = collection.createDocument(collectionLink, document, options, callback);

                            if (!isAccepted) {
                                getContext().getResponse().setBody(count);
                            }
                        };

    if (!jsonArray) {
        throw new Error("Input is Invalid");
    }

    let jsonCount = jsonArray.length;

    if (jsonCount === 0) {
        getContext().getResponse().setBody(0);
    }

    tryCreate(jsonArray[count], callback);
}