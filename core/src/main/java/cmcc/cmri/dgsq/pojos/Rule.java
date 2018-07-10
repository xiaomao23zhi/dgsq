package cmcc.cmri.dgsq.pojos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;

public class Rule implements Serializable {

    private static final Logger logger = LogManager.getLogger(Rule.class);

    //
    private String ruleId;

    //
    private String ruleCatagory;

    //
    private String ruleName;

    //
    private String ruleDescription;

    //
    private String ruleSql;

    public Rule() {
    }

    public static Logger getLogger() {
        return logger;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getRuleCatagory() {
        return ruleCatagory;
    }

    public void setRuleCatagory(String ruleCatagory) {
        this.ruleCatagory = ruleCatagory;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public String getRuleDescription() {
        return ruleDescription;
    }

    public void setRuleDescription(String ruleDescription) {
        this.ruleDescription = ruleDescription;
    }

    public String getRuleSql() {
        return ruleSql;
    }

    public void setRuleSql(String ruleSql) {
        this.ruleSql = ruleSql;
    }
}
