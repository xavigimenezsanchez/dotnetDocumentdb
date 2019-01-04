function setCreatedDate() {
    let document = getContext().getRequest().getBody();

    document.createTime = new Date();

    getContext().getRequest().setBody(document);
}