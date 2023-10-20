package se.idempiere.sie;

import org.compiere.model.*;
import org.compiere.process.*;
import org.compiere.util.DB;
import org.compiere.util.Env;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.logging.Level;
import java.math.*;

import org.notima.sie.SIEFileType4;
import org.notima.sie.SIEParseException;
import org.notima.sie.TransRec;
import org.notima.sie.VerRec;

public class ImportSIEFile extends SvrProcess {

	private Properties	m_ctx;
	private int			m_clientId;
	private int			m_orgId;
	private int			m_acctSchemaId;
	private String		m_fileName;
	private int			m_gljDocTypeId;		// Journal type id
	MGLCategory 		m_glCategory;	
	Map<String, MAccount> m_validCombMap;
	MAcctSchema 		m_acctSchema;
	private int			m_conversionTypeId;
	
	@Override
	protected void prepare() {

		m_ctx = Env.getCtx();
		m_clientId = Env.getAD_Client_ID(m_ctx);
		
        // Get parameters
        ProcessInfoParameter[] para = getParameter();
        for (int i = 0; i < para.length; i++) {
            String name = para[i].getParameterName();
            if (para[i].getParameter() == null);
              else if (name.equals("C_AcctSchema_ID")) {
				m_acctSchemaId = para[i].getParameterAsInt();
            } else if (name.equals("AD_Org_ID")) {
            	m_orgId = para[i].getParameterAsInt();
            } else if (name.equals("FileName")) {
            	m_fileName = para[i].getParameter().toString();
            } else {
                log.log(Level.SEVERE, "Unknown Parameter: " + name);
            }
        }
        
	}
	
	@Override
	protected String doIt() throws Exception {

		SIEFileType4 sieFile = new SIEFileType4(m_fileName);
		sieFile.readFile();
		createGLJournalBatchFromSie(sieFile);
		
		return("");
	}

	
	private void createGLJournalBatchFromSie(SIEFileType4 sieFile) throws SIEParseException {

		// TODO: Checks if balances are included etc etc.
		// TODO: Checks that all accounts are valid etc etc
		m_acctSchema = MAcctSchema.get (m_ctx, m_acctSchemaId);
		m_conversionTypeId = MConversionType.getDefault(m_clientId);		
        // Read all existing valid combinations for quick lookup
        MAccount validComb;
        List<MAccount> validCombs = new Query(
                    getCtx(),
                    MAccount.Table_Name,
                    "C_AcctSchema_ID=? and AD_Client_ID=? and (AD_Org_ID=0 or AD_Org_ID=?)",
                    get_TrxName())
                .setParameters(new Object[]{m_acctSchemaId, m_clientId, m_orgId})
                .list();

        m_validCombMap = new TreeMap<String, MAccount>();
        for (Iterator<MAccount> it = validCombs.iterator(); it.hasNext();) {
            validComb = it.next();
            m_validCombMap.put(validComb.getAccount().getValue(), validComb);
        }
        
        // Get document type for GLJ.
        MDocType[] gljDocTypes = MDocType.getOfDocBaseType(m_ctx, MDocType.DOCBASETYPE_GLJournal);
        // Use the first available
        if (gljDocTypes==null || gljDocTypes.length==0) {
        	throw new SIEParseException("No default GL Journal document type.");
        }
        m_gljDocTypeId = gljDocTypes[0].get_ID();
        
        m_glCategory = MGLCategory.getDefault(m_ctx, MGLCategory.CATEGORYTYPE_Import);        
        
        Vector<VerRec> vers = sieFile.getVerRecords();
        MJournalBatch batch;
        MJournal journal;
        VerRec v;
        for (Iterator<VerRec> it = vers.iterator(); it.hasNext(); ) {
        	v = it.next();
        	batch = new MJournalBatch(m_ctx, 0, get_TrxName());
    		java.sql.Timestamp verDate = new java.sql.Timestamp(v.getVerDatum().getTime());
    		batch.setC_DocType_ID(m_gljDocTypeId);
    		batch.setPostingType(MJournalBatch.POSTINGTYPE_Actual);
    		batch.setDateAcct(verDate);
    		batch.setDateDoc(verDate);
    		batch.setDescription(v.getVerText());
    		batch.setControlAmt(new BigDecimal(Double.toString(v.getTotalDebet())));
    		// Save batch to get a batch id
    		batch.saveEx(get_TrxName());
    		journal = createGLEntryFromSieVer(batch.get_ID(), v);
        }
		
	}
	
	private MJournal createGLEntryFromSieVer(int batchId, VerRec v) throws SIEParseException {
		
		String accountKey;
		MJournal journal = new MJournal(m_ctx, 0, get_TrxName());
		java.sql.Timestamp verDate = new java.sql.Timestamp(v.getVerDatum().getTime());
		journal.setGL_JournalBatch_ID(batchId);
		journal.setC_DocType_ID(m_gljDocTypeId);
		journal.setC_AcctSchema_ID(m_acctSchemaId);
		journal.setC_Currency_ID(m_acctSchema.getC_Currency_ID()); // Use the same currency as account schema
		journal.setC_ConversionType_ID(m_conversionTypeId);
		journal.setDateAcct(verDate);
		journal.setDateDoc(verDate);
		journal.setDescription(v.getVerText());
		journal.setControlAmt(new BigDecimal(Double.toString(v.getTotalDebet())));
		journal.setGL_Category_ID(m_glCategory.get_ID());
		rate(journal);
		setPeriod(journal);
		// Save journal to get a journal id
		journal.saveEx(get_TrxName());

		// Create journal lines from verification transactions
		Vector<TransRec> trans = v.getTransList();
		TransRec t;
		MJournalLine line;
		MAccount	account;
		for (Iterator<TransRec> it = trans.iterator(); it.hasNext(); ) {
			t = it.next();
			line = new MJournalLine(m_ctx, 0, get_TrxName());
			line.setGL_Journal_ID(journal.get_ID());
			// Lookup account
			accountKey = t.getKontoNr();
			account = m_validCombMap.get(accountKey);
			if (account==null) {
				account = addValidCombination(accountKey);
				if (account==null)
					throw new SIEParseException(accountKey + " does not exist in target chart of accounts");
			}
			line.setC_ValidCombination_ID(account);
			line.setC_Currency_ID(journal.getC_Currency_ID());
			line.setC_ConversionType_ID(journal.getC_ConversionType_ID());
			line.setCurrencyRate(journal.getCurrencyRate());
			line.setDateAcct(journal.getDateAcct());
			if (t.getBelopp()<0.0) {
				line.setAmtSourceCr(new BigDecimal(Double.toString(Math.abs(t.getBelopp()))));
			} else {
				line.setAmtSourceDr(new BigDecimal(Double.toString(t.getBelopp())));
			}
			line.saveEx(get_TrxName());
		}
		
		return journal;
	}

	/**
	 * Adds a new valid combination with the given element value for the current
	 * client and account schema.
	 * 
	 * @param elementValue
	 * @return	The valid combination (Account) if the creation is successful. Null
	 * 			if the combination could not be created (ie the element value doesn't exist).
	 */
	private MAccount addValidCombination(String elementValue) {
		
		MElementValue account = new Query(m_ctx, MElementValue.Table_Name, "value=?", get_TrxName())
								.setClient_ID()
								.setParameters(new Object[]{elementValue})
								.first();

		if (account==null) return(null); // No matching element value
		
		MAccount newValidComb = MAccount.get(m_ctx, m_clientId, m_orgId, 
											 m_acctSchemaId, account.get_ID(), 0,
											 0,0,0,
											 0,0,0,
											 0,0,0,
											 0,0,0,0);
		
		return(newValidComb);
		
	}
	
	private void rate(MJournal journal) {
		//  Source info
		int C_Currency_ID = journal.getC_Currency_ID();
		int C_ConversionType_ID = journal.getC_ConversionType_ID();
		Timestamp DateAcct = journal.getDateAcct();
		if (DateAcct == null)
			DateAcct = new Timestamp(System.currentTimeMillis());
		//
		int C_AcctSchema_ID = journal.getC_AcctSchema_ID();
		MAcctSchema as = MAcctSchema.get (m_ctx, C_AcctSchema_ID);

		BigDecimal CurrencyRate = MConversionRate.getRate(C_Currency_ID, as.getC_Currency_ID(), 
			DateAcct, C_ConversionType_ID, m_clientId, m_orgId);
		if (CurrencyRate == null)
			CurrencyRate = Env.ZERO;
		journal.setCurrencyRate(CurrencyRate);
	}
	
	private void setPeriod(MJournal journal) throws SIEParseException {

		int C_Period_ID = 0;
		String sql = "SELECT C_Period_ID "
			+ "FROM C_Period "
			+ "WHERE C_Year_ID IN "
			+ "	(SELECT C_Year_ID FROM C_Year WHERE C_Calendar_ID ="
			+ "  (SELECT C_Calendar_ID FROM AD_ClientInfo WHERE AD_Client_ID=?))"
			+ " AND ? BETWEEN StartDate AND EndDate"
			+ " AND IsActive='Y'"
			+ " AND PeriodType='S'";
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement(sql, null);
			pstmt.setInt(1, m_clientId);
			pstmt.setTimestamp(2, journal.getDateAcct());
			rs = pstmt.executeQuery();
			if (rs.next())
				C_Period_ID = rs.getInt(1);
			rs.close();
			pstmt.close();
			pstmt = null;
		}
		catch (SQLException e)
		{
			log.log(Level.SEVERE, sql, e);
			new SIEParseException(e.getMessage());
		}
		finally
		{
			DB.close(rs, pstmt);
		}
		if (C_Period_ID != 0)
			journal.setC_Period_ID(C_Period_ID);
		
	}

}
