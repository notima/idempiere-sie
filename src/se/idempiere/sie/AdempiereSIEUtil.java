package se.idempiere.sie;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

import org.compiere.model.MCalendar;
import org.compiere.model.MClientInfo;
import org.compiere.model.MOrgInfo;
import org.compiere.model.MPeriod;
import org.compiere.model.MYear;
import org.compiere.model.Query;
import org.compiere.util.DB;
import org.compiere.util.Trx;
import org.notima.sie.AccountRec;
import org.notima.sie.BalanceRec;
import org.notima.sie.RARRec;
import org.notima.sie.ResRec;
import org.notima.sie.SIEFile;
import org.notima.sie.SIEFileType4;
import org.notima.sie.TransRec;
import org.notima.sie.VerRec;


public class AdempiereSIEUtil {

	/**
	 * Get calendar dates
	 * 
	 * @param	untilDate	The last date of accounting.
	 * @param	years		Years including the one in untildate
	 * 
	 */
	public static List<RARRec> getFiscalYears(Properties ctx, int clientId, int orgId, java.util.Date untilDate, int years, String trxName) throws Exception {
		
		List<RARRec> result = new ArrayList<RARRec>();
		
		MCalendar cal = null;
		MOrgInfo info = null;
		MClientInfo clientInfo;
		
		if (orgId!=0) {
			info = MOrgInfo.get(ctx, orgId, trxName);
			cal = (MCalendar)info.getC_Calendar();
		}
		if (cal==null) {
			// Get calendar from client
			if (clientId==0 && info!=null) {
				clientId = info.getAD_Client_ID();
			}
			if (clientId!=0) {
				clientInfo = MClientInfo.get(ctx, clientId);
				cal = (MCalendar)clientInfo.getC_Calendar();
			}
		}
		// If calendar is null here, there's not calendar.
		if (cal==null || cal.get_ID()==0) {
			throw new Exception("Can't find default calendar for Org/Client");
		}

		// Get end for calendar
		MPeriod endPeriod = MPeriod.findByCalendar(ctx, new java.sql.Timestamp(untilDate.getTime()), cal.get_ID(), trxName);
		// Get year
		MYear endYear = (MYear)endPeriod.getC_Year();

		RARRec rec = toRARRec(endYear);
		rec.setRarNo(0);
		result.add(rec);
		
		if (years>0) {
			
			Calendar cal2 = Calendar.getInstance();
			cal2.setTime(untilDate);
			cal2.add(Calendar.YEAR, -1);
			
			for (int i=1; i<=years; i++) {
				endPeriod = MPeriod.findByCalendar(ctx, new java.sql.Timestamp(cal2.getTimeInMillis()), cal.get_ID(), trxName);
				endYear = (MYear)endPeriod.getC_Year();
				rec = toRARRec(endYear);
				rec.setRarNo(-i);
				result.add(rec);
				cal2.add(Calendar.YEAR, -1);
			}
			
		}
		
		return result;
	}
	
	public static RARRec toRARRec(MYear year) {
		
		RARRec rec = new RARRec();
		
		List<MPeriod> periods = new Query(year.getCtx(), MPeriod.Table_Name, "C_Year_ID=?", year.get_TrxName())
			.setParameters(new Object[]{year.get_ID()})
			.setOrderBy(MPeriod.COLUMNNAME_StartDate)
			.list();

		MPeriod first = periods.get(0);
		MPeriod last = periods.get(periods.size()-1);
		
		rec.setStartDate(new java.sql.Date(first.getStartDate().getTime()));
		rec.setEndDate(new java.sql.Date(last.getEndDate().getTime()));
		
		return rec;
		
	}

	/**
	 * Create account list, result and balances
	 * 
	 * TODO: Add support for SRU
	 * 
	 * @param rar		Fiscal year
	 * @param fromDate	If from date is different from fiscal year start
	 * @param toDate	If end date is different from fiscal year end
	 * @param acctSchemaId
	 * @param orgId
	 * @param trxName
	 */
	public static void createAccountList(RARRec rar,
			java.sql.Timestamp fromDate,
			java.sql.Timestamp toDate,
			int acctSchemaId, 
			int orgId, 
			SIEFile sieFile, 
			String trxName) throws Exception {

		if (fromDate==null)
			fromDate = new java.sql.Timestamp(rar.getStartDate().getTime());
		if (toDate==null)
			toDate = new java.sql.Timestamp(rar.getEndDate().getTime());
		
		if (sieFile==null) throw new Exception("SIE file must be supplied. Can't be null");
		
		// Create SQL query for checking the in balance for all accounts
		StringBuffer sql = new StringBuffer(); 
		sql.append("select e.value, e.name, e.accounttype, ac.Account_ID, sum(ac.amtacctdr-ac.amtacctcr) as balance from fact_acct ac " + 
				   "left join c_elementvalue e on (ac.account_id=e.c_elementvalue_id) " +
				   "where ac.C_AcctSchema_ID=? AND ac.PostingType='A' ");
		if (orgId!=0) {
				   sql.append("AND ac.AD_Org_ID=? ");
		}
		sql.append(" AND TRUNC(ac.DateAcct) < " + DB.TO_DATE(fromDate, true) );
		sql.append(" GROUP BY e.value, e.name, e.accounttype, ac.Account_ID");
		sql.append(" ORDER BY e.value");

		boolean createNew = trxName==null;
		if (trxName==null) {
			trxName = Trx.createTrxName();
		}
		Trx trx = Trx.get(trxName, createNew);
		Connection conn = trx.getConnection();
		PreparedStatement ps  = conn.prepareStatement(sql.toString());
		int c=1;
		ps.setInt(c++, acctSchemaId);
		if (orgId!=0)
			ps.setInt(c++, orgId);

		// Run query to check IB
		AccountRec ar;
		BalanceRec br;
		ResRec rr;
		String acctNo;
		String acctName;
		String acctType;
		double balance;
		
		ResultSet rs = ps.executeQuery();
		
		while(rs.next()) {
			acctNo = rs.getString(1);
			acctName = rs.getString(2);
			acctType = rs.getString(3);
			balance = rs.getDouble(5);
			ar = new AccountRec(acctNo, acctName);
			sieFile.addAccountRecord(ar);
			if (balance!=0) {
				if ("O".equals(acctType) || "A".equals(acctType) || "L".equals(acctType)) {
					br = new BalanceRec(acctNo, balance);
					br.setYearOffset(rar.getRarNo());
					sieFile.addBalanceRecord(br);
				} else {
					rr = new ResRec(acctNo, balance);
					rr.setYearOffset(rar.getRarNo());
					sieFile.addResultRecord(rr);
				}
			}
		}

		rs.close();
		ps.close();
		
		// Create SQL query for checking the out balance for all accounts
		sql = new StringBuffer(); 
		sql.append("select e.value, e.name, e.accounttype, ac.Account_ID, sum(ac.amtacctdr-ac.amtacctcr) as balance from fact_acct ac " + 
				   "left join c_elementvalue e on (ac.account_id=e.c_elementvalue_id) " +
				   "where ac.C_AcctSchema_ID=? AND ac.PostingType='A' ");
		if (orgId!=0) {
				   sql.append("AND ac.AD_Org_ID=? ");
		}
		sql.append(" AND TRUNC(ac.DateAcct) <= " + DB.TO_DATE(toDate, true) );
		sql.append(" GROUP BY e.value, e.name, e.accounttype, ac.Account_ID");
		sql.append(" ORDER BY e.value");

		ps  = conn.prepareStatement(sql.toString());
		c=1;
		ps.setInt(c++, acctSchemaId);
		if (orgId!=0)
			ps.setInt(c++, orgId);
		
		rs = ps.executeQuery();
		while(rs.next()) {
			acctNo = rs.getString(1);
			acctName = rs.getString(2);
			acctType = rs.getString(3);
			balance = rs.getDouble(5);
			ar = new AccountRec(acctNo, acctName);
			sieFile.addAccountRecord(ar);
			if (balance!=0) {
				if ("O".equals(acctType) || "A".equals(acctType) || "L".equals(acctType)) { 
					br = new BalanceRec(acctNo, balance);
					br.setInBalance(false); // UB
					br.setYearOffset(rar.getRarNo());
					sieFile.addBalanceRecord(br);
					
				} else {
					rr = new ResRec(acctNo, balance);
					rr.setYearOffset(rar.getRarNo());
					sieFile.diffResultRecord(rr);
				}
			}
		}
		
		rs.close();
		ps.close();
		
		/*
		sql.append(" AND TRUNC(DateAcct) BETWEEN ")
			.append(DB.TO_DATE(p_Date_From, true))
			.append(" AND ")
			.append(DB.TO_DATE(p_Date_To, true));
			*/
		
		
		if(createNew)
			conn.close();
		
		
	}

	/**
	 * Creates transaction records
	 * 
	 * @param fromDate
	 * @param toDate
	 * @param acctSchemaId
	 * @param orgId
	 * @param sieFile
	 * @param trxName
	 * @throws Exception
	 */
	public static void createVerRecs(java.sql.Timestamp fromDate, 
			  java.sql.Timestamp toDate,
			  int acctSchemaId, 
			  int orgId, 
			  SIEFileType4 sieFile, 
			  String trxName) throws Exception {
		
		StringBuffer sql = new StringBuffer(
				"select e.value, ac.dateacct, ac.record_id, ac.ad_table_id, ac.amtacctdr, ac.amtacctcr, ac.description, ac.created " + 
				"from fact_acct ac " + 
				"left join c_elementvalue e on (ac.account_id=e.c_elementvalue_id) " + 
				"where ac.c_acctschema_id=" + acctSchemaId + " " +  
				"and date_trunc('day',ac.dateacct)>=" + DB.TO_DATE(fromDate, true) + " " +
				"and date_trunc('day',ac.dateacct)<=" + DB.TO_DATE(toDate, true) + " and ac.PostingType='A' " +  
				"order by ac.ad_table_id, ac.record_id"
				);

		boolean createNew = trxName==null;
		if (trxName==null) {
			trxName = Trx.createTrxName();
		}
		Trx trx = Trx.get(trxName, createNew);
		Connection conn = trx.getConnection();
		PreparedStatement ps  = conn.prepareStatement(sql.toString());

		ResultSet rs = ps.executeQuery();
		String acct;
		java.sql.Date dateAcct;
		java.sql.Date dateReg;
		int recordId;
		int tableId;
		double debet;
		double credit;
		String desc;
		String transDesc;
		
		VerRec v = null;
		TransRec tr = null;
		
		while(rs.next()) {
			acct = rs.getString(1);
			dateAcct = rs.getDate(2);
			recordId = rs.getInt(3);
			tableId = rs.getInt(4);
			debet = rs.getDouble(5);
			credit = rs.getDouble(6);
			desc = rs.getString(7);
			dateReg = rs.getDate(8);

			// New series
			if (v==null ||
				(v!=null && (!Integer.toString(tableId).equals(v.getSerie()) || !Integer.toString(recordId).equals(v.getVerNr())))) {
				if (v!=null) {
					sieFile.addVerRecord(v);
				}
				v = new VerRec();
				v.setVerDatum(dateAcct);
				v.setRegDatum(dateReg);
				v.setSerie(Integer.toString(tableId));
				v.setVerNr(Integer.toString(recordId));
				v.setVerText(desc);
			}
			// Add transaction record
			if (debet!=credit) {
				if (desc!=null && v.getVerText()!=null && !desc.equals(v.getVerText())) {
					transDesc = desc;
				} else {
					transDesc = null;
				}
				tr = new TransRec(acct, debet-credit, dateAcct, transDesc);
				v.addTransRec(tr);
			}
		}
		if (v!=null) {
			// Add the last record
			sieFile.addVerRecord(v);
		}
		rs.close();
		ps.close();

		if(createNew)
			conn.close();
		
	}
}
