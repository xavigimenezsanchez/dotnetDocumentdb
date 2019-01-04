function getTotalFunding(company) {
    let raiseAdmount = 0;

    if (!company || !company.funding_rounds) {
        return 0;
    }

    let arrayLengt = company.funding_rounds.length;

    for (let i = 0; i < arrayLengt; i++) {
        raiseAdmount += company.funding_rounds[i].raised_amount;
    }

    return raiseAdmount;
}