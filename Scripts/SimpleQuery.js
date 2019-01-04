function simple(whereClause) {
    let collection = getContext().getCollection();

    let isAccepted = collection.queryDocuments(
                    collection.getSelfLink(),
                    "Select * from root r" + whereClause,
                    (err, result, options) => {
                        if (err) {
                            throw err;
                        }

                        if (!result || !result.length) {
                            getContext().getResponse().setBody("Query Returnd no Results");
                        } else {
                            getContext().getResponse().setBody(JSON.stringify(result));
                        }
                    }
    );

    if (!isAccepted) {
        throw new Error("Something went wrong");
    }
}