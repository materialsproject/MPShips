# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import json
import numpy as np
from itertools import groupby
from scipy.optimize import brentq
from mpships.redox_thermo_csp.redox_utils import (
    remove_comp_one,
    add_comp_one,
    rootfind,
    get_energy_data,
    energy_on_the_fly,
    s_th_o,
    dh_ds,
    funciso,
    funciso_redox,
    funciso_theo,
    isobar_line_elling,
    funciso_redox_theo,
    d_h_num_dev_calc,
    d_s_fundamental,
)


class InitData:
    def init_load_json(filename="theo_data.json"):
        """
        Loads materials data from json files.
        This data is from 2018, but since the paper was based on this data, it is recommended to use this old data for consistency.
        :param filename: Name or path to the file (str)
        :return:
        json_out: Loaded data
        """
        with open(filename, "r") as f:
            theo_data = json.load(f)
        return theo_data

    def all_float_dict(dictionary):
        """make sure the dicts contain all number values as floats; input: dict"""
        return {
            k: (
                float(v)
                if (type(v) == str and v.replace(".", "").replace("-", "").isdigit())
                else v
            )
            for k, v in dictionary.items()
        }

    def init_isographs(theo_data, exp_data, compstr=None, cid=None):
        theo_data = theo_data[
            "collection"
        ]  # this is just necessary due to the structure of the theo_data dict
        chosen_entry = []
        if not compstr and not cid:
            raise ValueError("Either composition or contribution ID must be specified")
        else:
            for entry in theo_data:  # find matching entries
                if compstr:
                    # reformat user input so the compstr always contains compositions of 1 ("SrFeOx" -> "Sr1Fe1Ox")
                    compstr_1 = (
                        add_comp_one(compstr)
                        .replace("[", "")
                        .replace("]", "")
                        .replace(",", "")
                        .replace("'", "")
                    )
                    if (entry["pars"]["theo_compstr"] == compstr) or (
                        entry["pars"]["theo_compstr"] == compstr_1
                    ):
                        chosen_entry = entry
                        break
                if cid and entry["_id"] == cid:
                    chosen_entry = entry
                    break
        if not chosen_entry:
            raise ValueError("Entry not found")

        data = chosen_entry["data"]
        pars = chosen_entry["pars"]
        if not compstr:
            compstr = pars["theo_compstr"]
        pars["compstr_disp"] = remove_comp_one(compstr)

        experiment = {}  # get experimental data if available
        if "Exp" in pars["data_availability"]:
            pars["experimental_data_available"] = True
            experiment = {}
            for n in range(len(exp_data["theo_compstr"])):
                exp_dat_here = {k: v[n] for k, v in exp_data.items()}
                if (
                    exp_dat_here["theo_compstr"] == compstr_1
                    or exp_dat_here["theo_compstr"] == compstr
                ):
                    experiment = exp_dat_here
                    break
            pars["compstr_exp"] = data["oxidized_phase"]["composition"]
            pars["compstr_exp"] = [
                "".join(g) for _, g in groupby(str(pars["compstr_exp"]), str.isalpha)
            ]
        else:
            pars["experimental_data_available"] = False
            pars["compstr_exp"] = "n.a."

        pars["td_perov"] = pars["elastic"]["Debye temp perovskite"]
        pars["td_brownm"] = pars["elastic"]["Debye temp brownmillerite"]
        pars["tens_avail"] = pars["elastic"]["Elastic tensors available"]

        if experiment:
            # some restructuring due to the original arrangement of variables in the solar perovskite code
            # check solar_perovskite.isographs.Experimental.get_fit_parameters
            pars["fit_param_enth"] = {}
            pars["fit_param_enth"]["a"] = float(experiment["dH_max"])
            pars["fit_param_enth"]["b"] = float(experiment["dH_min"])
            pars["fit_param_enth"]["c"] = float(experiment["t"])
            pars["fit_param_enth"]["d"] = float(experiment["s"])
            pars["fit_par_ent"] = {}
            if experiment["fit type entropy"] == "Solid_Solution":
                pars["fit_par_ent"]["a"] = float(experiment["entr_solid_sol_s"])
                pars["fit_par_ent"]["b"] = float(experiment["entr_solid_sol_shift"])
            else:
                pars["fit_par_ent"]["a"] = float(experiment["entr_dil_s_v"])
                pars["fit_par_ent"]["b"] = float(experiment["entr_dil_a"])
            pars["fit_par_ent"]["c"] = float(experiment["delta_0"])
            pars["fit_type_entr"] = experiment["fit type entropy"]
            pars["delta_0"] = float(experiment["delta_0"])
            pars["delta_min"] = float(experiment["min_delta_exp"])
            pars["delta_max"] = float(experiment["max_delta_exp"])
            # hard-coded parameters for entropy fit of SeFeO3-delta
            pars["fit_param_fe"] = {}
            pars["fit_param_fe"]["a"] = 2.310619e2
            pars["fit_param_fe"]["b"] = -2.433376e1
            pars["fit_param_fe"]["c"] = 8.3978465e-1
            pars["fit_param_fe"]["d"] = 2.191566199e-1

        # There was a factor of 1000 difference between the original data from
        # the json file and what was uploaded to mp_contribs. This fixes that.
        return InitData.all_float_dict(data), InitData.all_float_dict(pars)


class Isographs:
    def __init__(self, compstr, plottype, iso, rng, a=1e-10, b=0.5 - 1e-10):
        self.compstr = compstr
        self.plottype = plottype
        self.iso = iso
        self.rng = rng
        self.a = a
        self.b = b

    def prepare_limits(self):
        """Prepares x values and limits for the plots"""
        # this variable being called "payload" is due to the legacy script on old MPContribs (2018)
        payload = {}
        payload["iso"] = self.iso
        payload["rng"] = self.rng

        if self.plottype == "isotherm":  # pressure on the x-axis
            x_val = np.log(np.logspace(payload["rng"][0], payload["rng"][1], num=100))
        elif (
            self.plottype == "dH" or self.plottype == "dS"
        ):  # dH or dS, delta on the x-axis
            x_val = np.linspace(0.01, 0.49, num=100)
        else:  # temperature on the x-axis
            x_val = np.linspace(payload["rng"][0], payload["rng"][1], num=100)

        return payload, x_val

    def isographs(self, pars, payload, x_val):
        a, b = self.a, self.b  # limiting values for non-stoichiometry delta in brentq
        resiso, resiso_theo = [], []
        if pars[
            "experimental_data_available"
        ]:  # only execute this if experimental data is available
            for xv in x_val:  # calculate experimental data
                try:
                    if self.plottype == "isotherm":
                        s_th = s_th_o(payload["iso"])
                        args = (xv, payload["iso"], pars, s_th)
                    else:
                        s_th = s_th_o(xv)
                        args = (payload["iso"], xv, pars, s_th)
                    if self.plottype == "isoredox":
                        solutioniso = brentq(funciso_redox, -300, 300, args=args)
                        resiso.append(np.exp(solutioniso))
                    else:
                        solutioniso = rootfind(a, b, args, funciso)
                        resiso.append(solutioniso)
                except (
                    ValueError
                ):  # if brentq function finds no zero point due to plot out of range
                    resiso.append(None)
            # show interpolation
            res_interp, res_fit = [], []
            for i in range(len(resiso)):
                if self.plottype == "isoredox":
                    delta_val = payload["iso"]
                else:
                    delta_val = resiso[i]
                if not delta_val:
                    delta_val = -10  # dummy to not throw an error in next row
                if pars["delta_min"] < delta_val < pars["delta_max"]:
                    res_fit.append(resiso[i])
                    res_interp.append(None)
                else:
                    res_fit.append(None)
                    res_interp.append(resiso[i])
        else:
            res_fit, res_interp = (
                None,
                None,
            )  # don't plot any experimental data if it is not available

        try:  # calculate theoretical data
            for xv in x_val[
                ::4
            ]:  # use less data points for theoretical graphs to improve speed
                if self.plottype == "isotherm":
                    args_theo = (
                        xv,
                        payload["iso"],
                        pars,
                        pars["td_perov"],
                        pars["td_brownm"],
                        pars["dh_min"] * 1000,
                        pars["dh_max"] * 1000,
                        pars["act_mat"],
                    )
                else:
                    args_theo = (
                        payload["iso"],
                        xv,
                        pars,
                        pars["td_perov"],
                        pars["td_brownm"],
                        pars["dh_min"] * 1000,
                        pars["dh_max"] * 1000,
                        pars["act_mat"],
                    )
                if self.plottype == "isoredox":
                    try:
                        solutioniso_theo = brentq(
                            funciso_redox_theo, -300, 300, args=args_theo
                        )
                    except ValueError:
                        solutioniso_theo = brentq(
                            funciso_redox_theo, -100, 100, args=args_theo
                        )
                    resiso_theo.append(np.exp(solutioniso_theo))
                else:
                    solutioniso_theo = rootfind(a, b, args_theo, funciso_theo)
                    resiso_theo.append(solutioniso_theo)
        except (
            ValueError
        ):  # if brentq function finds no zero point due to plot out of range
            resiso_theo.append(None)
        if self.plottype == "isotherm":
            x = list(np.exp(x_val))
        else:
            x = list(x_val)
        x_theo = x[::4]
        x_exp = None
        if pars["experimental_data_available"]:
            x_exp = x
        response = [
            {
                "x": x_exp,
                "y": res_fit,
                "name": "exp_fit",
                "line": {"color": "rgb(5,103,166)", "width": 2.5},
            },
            {
                "x": x_exp,
                "y": res_interp,
                "name": "exp_interp",
                "line": {"color": "rgb(5,103,166)", "width": 2.5, "dash": "dot"},
            },
            {
                "x": x_theo,
                "y": resiso_theo,
                "name": "theo",
                "line": {"color": "rgb(217,64,41)", "width": 2.5},
            },
            [0, 0],
            [
                pars["compstr_disp"],
                pars["compstr_exp"],
                pars["tens_avail"],
                pars["last_updated"],
            ],
        ]
        return response

    def enthalpy_entropy(self, pars, payload, x_val):
        resiso, resiso_theo = [], []
        if pars[
            "experimental_data_available"
        ]:  # only execute this if experimental data is available
            for xv in x_val:  # calculate experimental data
                try:
                    s_th = s_th_o(payload["iso"])
                    args = (payload["iso"], xv, pars, s_th)
                    solutioniso = dh_ds(xv, args[-1], args[-2])
                    if self.plottype == "dH":
                        solutioniso = solutioniso[0] / 1000
                    else:
                        solutioniso = solutioniso[1]
                    resiso.append(solutioniso)
                except (
                    ValueError
                ):  # if brentq function finds no zero point due to plot out of range
                    resiso.append(None)
            res_interp, res_fit = [], []
            for delta_val, res_i in zip(x_val, resiso):  # show interpolation
                if not delta_val:
                    delta_val = -10  # dummy to not throw an error in next row
                if (
                    pars["delta_min"] < delta_val < pars["delta_max"]
                ):  # result within experimentally covered delta range
                    res_fit.append(res_i)
                    res_interp.append(None)
                else:  # result outside this range
                    res_fit.append(None)
                    res_interp.append(res_i)
        else:
            res_fit, res_interp = (
                None,
                None,
            )  # don't plot any experimental data if it is not available

        try:  # calculate theoretical data
            for xv in x_val[
                ::4
            ]:  # use less data points for theoretical graphs to improve speed
                (
                    payload["iso"],
                    xv,
                    pars,
                    pars["td_perov"],
                    pars["td_brownm"],
                    pars["dh_min"] * 1000,
                    pars["dh_max"] * 1000,
                    pars["act_mat"],
                )
                if self.plottype == "dH":
                    solutioniso_theo = (
                        d_h_num_dev_calc(
                            delta=xv,
                            dh_1=pars["dh_min"] * 1000,
                            dh_2=pars["dh_max"] * 1000,
                            temp=payload["iso"],
                            act=pars["act_mat"],
                        )
                        / 1000
                    )
                else:
                    solutioniso_theo = d_s_fundamental(
                        delta=xv,
                        dh_1=pars["dh_min"] * 1000,
                        dh_2=pars["dh_max"] * 1000,
                        temp=payload["iso"],
                        act=pars["act_mat"],
                        t_d_perov=pars["td_perov"],
                        t_d_brownm=pars["td_brownm"],
                    )
                resiso_theo.append(solutioniso_theo)
        except (
            ValueError
        ):  # if brentq function finds no zero point due to plot out of range
            resiso_theo.append(None)

        x = list(x_val)
        x_theo = x[::4]
        x_exp = None
        if pars["experimental_data_available"]:
            x_exp = x

        # limiting values for the plot,
        y_max = max(np.append(resiso, resiso_theo)) * 1.2
        if self.plottype == "dH":
            if max(np.append(resiso, resiso_theo)) > (pars["dh_max"] * 1000 * 0.0015):
                y_max = pars["dh_max"] * 1000 * 0.0015
        else:
            if max(np.append(resiso, resiso_theo)) > 250:
                y_max = 250
        if self.plottype == "dH" and min(np.append(resiso, resiso_theo)) > -10:
            y_min = min(np.append(resiso, resiso_theo)) * 0.8
        else:
            y_min = -10
        response = [
            {
                "x": x_exp,
                "y": res_fit,
                "name": "exp_fit",
                "line": {"color": "rgb(5,103,166)", "width": 2.5},
            },
            {
                "x": x_exp,
                "y": res_interp,
                "name": "exp_interp",
                "line": {"color": "rgb(5,103,166)", "width": 2.5, "dash": "dot"},
            },
            {
                "x": x_theo,
                "y": resiso_theo,
                "name": "theo",
                "line": {"color": "rgb(217,64,41)", "width": 2.5},
            },
            [y_min, y_max],
            [
                pars["compstr_disp"],
                pars["compstr_exp"],
                pars["tens_avail"],
                pars["last_updated"],
            ],
        ]
        return response

    def ellingham(self, pars, payload, x_val, delta):
        iso = np.log(10 ** payload["iso"])
        resiso, resiso_theo, ellingiso = [], [], []
        if pars[
            "experimental_data_available"
        ]:  # only execute this if experimental data is available
            for xv in x_val:  # calculate experimental data
                try:
                    s_th = s_th_o(xv)
                    args = (iso, xv, pars, s_th)
                    solutioniso = (
                        dh_ds(delta, args[-1], args[-2])[0]
                        - dh_ds(delta, args[-1], args[-2])[1] * xv
                    ) / 1000
                    resiso.append(solutioniso)
                    ellingiso_i = isobar_line_elling(args[0], xv) / 1000
                    ellingiso.append(ellingiso_i)
                except (
                    ValueError
                ):  # if brentq function finds no zero point due to plot out of range
                    resiso.append(None)

            res_interp, res_fit = [], []
            for delta_val, res_i in zip(x_val, resiso):  # show interpolation
                if not delta_val:
                    delta_val = -10  # dummy to not throw an error in next row
                if (
                    pars["delta_min"] < delta_val < pars["delta_max"]
                ):  # result within experimentally covered delta range
                    res_fit.append(res_i)
                    res_interp.append(None)
                else:  # result outside this range
                    res_fit.append(None)
                    res_interp.append(res_i)
        else:
            res_fit, res_interp = (
                None,
                None,
            )  # don't plot any experimental data if it is not available

        try:  # calculate theoretical data
            for xv in x_val[
                ::4
            ]:  # use less data points for theoretical graphs to improve speed
                dh = d_h_num_dev_calc(
                    delta=delta,
                    dh_1=pars["dh_min"] * 1000,
                    dh_2=pars["dh_max"] * 1000,
                    temp=xv,
                    act=pars["act_mat"],
                )
                ds = d_s_fundamental(
                    delta=delta,
                    dh_1=pars["dh_min"] * 1000,
                    dh_2=pars["dh_max"] * 1000,
                    temp=xv,
                    act=pars["act_mat"],
                    t_d_perov=pars["td_perov"],
                    t_d_brownm=pars["td_brownm"],
                )
                solutioniso_theo = (dh - ds * xv) / 1000
                resiso_theo.append(solutioniso_theo)
        except (
            ValueError
        ):  # if brentq function finds no zero point due to plot out of range
            resiso_theo.append(None)

        x = list(x_val)
        x_theo = x[::4]
        if pars["experimental_data_available"]:
            x_exp = x
        else:
            x_exp = None
            for xv in x_theo:
                ellingiso_i = isobar_line_elling(iso, xv) / 1000
                ellingiso.append(ellingiso_i)
        response = [
            {
                "x": x_exp,
                "y": res_fit,
                "name": "exp_fit",
                "line": {"color": "rgb(5,103,166)", "width": 2.5},
            },
            {
                "x": x_exp,
                "y": res_interp,
                "name": "exp_interp",
                "line": {"color": "rgb(5,103,166)", "width": 2.5, "dash": "dot"},
            },
            {
                "x": x_theo,
                "y": resiso_theo,
                "name": "theo",
                "line": {"color": "rgb(217,64,41)", "width": 2.5},
            },
            {
                "x": x_theo,
                "y": ellingiso,
                "name": "isobar line",
                "line": {"color": "rgb(100,100,100)", "width": 2.5},
            },
            [
                pars["compstr_disp"],
                pars["compstr_exp"],
                pars["tens_avail"],
                pars["last_updated"],
            ],
        ]
        return response


def energy_analysis(en_dat, payload):
    # parameters for the database ID
    payload["data_source"] = (
        "Theo" if payload["data_source"] == "Theoretical" else "Exp"
    )
    for k, v in payload.items():
        if not ((k == "pump_ener") or (k == "mech_env")):
            try:
                payload[k] = float(v)
            except ValueError:
                continue
    pump_ener = float(payload["pump_ener"].split("/")[0])
    cutoff = int(
        payload["cutoff"]
    )  # this sets the number of materials to display in the graph
    mech_env = bool(payload["mech_env"])
    if mech_env:
        pump_ener = -1
    param_disp = payload["param_disp"]

    # get the standardized results
    resdict = get_energy_data(
        en_dat,
        process_type=payload["process_type"],
        t_ox=payload["t_ox"],
        t_red=payload["t_red"],
        p_ox=payload["p_ox"],
        p_red=payload["p_red"],
        data_source=payload["data_source"],
        enth_steps=20,
    )

    response = [{"x": None, "y": None, "name": None, "type": "bar"} for i in range(4)]

    try:  # calculate specific results on the fly
        results = energy_on_the_fly(
            process=payload["process_type"],
            resdict=resdict,
            pump_ener=pump_ener,
            w_feed=payload["w_feed"],
            h_rec=payload["h_rec"],
            h_rec_steam=payload["steam_h_rec"],
            p_ox_wscs=payload["p_ox"],
        )

        prodstr = resdict[0]["energy_analysis"][0]["prodstr"]
        prodstr_alt = resdict[0]["energy_analysis"][0]["prodstr_alt"]

        if param_disp == "kJ/mol of product":
            param_disp = str("kJ/mol of " + prodstr_alt)
        elif param_disp == "kJ/L of product":
            param_disp = str("kJ/L of " + prodstr)
        elif param_disp == "Wh/L of product":
            param_disp = str("Wh/L of " + prodstr)
        elif param_disp == "mol product per mol redox material":
            param_disp = str("mol " + prodstr_alt + " per mol redox material")
        elif param_disp == "L product per mol redox material":
            param_disp = str("L " + prodstr + " per mol redox material")
        elif param_disp == "g product per mol redox material":
            param_disp = str("g " + prodstr + " per mol redox material")
        result = results[param_disp]

        commonname = (
            param_disp
            + ", \nT(ox)= "
            + str(payload["t_ox"])
            + " °C, T(red) = "
            + str(payload["t_red"])
        )
        if payload["process_type"] == "Air Separation":
            titlestr = (
                commonname
                + " °C, p(ox)= "
                + str(payload["p_ox"])
                + " bar, p(red) = "
                + str(payload["p_red"])
                + " bar"
            )
        elif payload["process_type"] == "CO2 Splitting":
            titlestr = (
                commonname
                + " °C, pCO/pCO2(ox)= "
                + str(payload["p_ox"])
                + ", p(red) = "
                + str(payload["p_red"])
                + " bar"
            )
        else:  # Water Splitting
            titlestr = (
                commonname
                + " °C, pH2/pH2O(ox)= "
                + str(payload["p_ox"])
                + ", p(red) = "
                + str(payload["p_red"])
                + " bar"
            )

        # remove duplicates (if any)
        rem_pos = -1
        for elem in range(len(result)):
            if elem > 0 and (result[elem][-1] == result[elem - 1][-1]):
                to_remove = result[elem]
                rem_pos = elem
        if rem_pos > -1:
            result = [i for i in result if str(i) != str(to_remove)]
            result.insert(rem_pos - 1, to_remove)

        result = [
            i for i in result if "inf" not in str(i[0])
        ]  # this removes all inf values
        result_part = result[:cutoff] if cutoff < len(result) else result

        if (
            len(result_part[0]) == 2
        ):  # output if only one y-value per material is displayed
            response[0]["x"] = [i[-1] for i in result_part]
            response[0]["y"] = (
                np.array([i[0] for i in result_part]).astype(float).tolist()
            )
            response[0]["name"] = param_disp
            if "non-stoichiometry" in param_disp:
                response[0]["name"] = (
                    response[0]["name"].split("between")[0] + " (Δδ)"
                )  # otherwise would be too long for y-axis label
            if "Mass change" in param_disp:
                response[0]["name"] = "mass change (%)"
            if "Heat to fuel efficiency" in param_disp:
                response[0]["name"] = "Heat to fuel efficiency (%)"

        else:  # display multiple values (such as chemical energy, sensible energy, ...)
            response[0]["x"] = [i[-1] for i in result_part]
            response[0]["y"] = (
                np.array([i[1] for i in result_part]).astype(float).tolist()
            )
            response[0]["name"] = "Chemical Energy"
            response[1]["x"] = [i[-1] for i in result_part]
            response[1]["y"] = (
                np.array([i[2] for i in result_part]).astype(float).tolist()
            )
            response[1]["name"] = "Sensible Energy"
            response[2]["x"] = [i[-1] for i in result_part]
            response[2]["y"] = (
                np.array([i[3] for i in result_part]).astype(float).tolist()
            )
            response[2]["name"] = "Pumping Energy"
            if payload["process_type"] == "Water Splitting":
                response[3]["x"] = [i[-1] for i in result_part]
                response[3]["y"] = (
                    np.array([i[4] for i in result_part]).astype(float).tolist()
                )
                response[3]["name"] = "Steam Generation"
        response[0].update({"title": titlestr, "yaxis_title": param_disp})

    except IndexError:  # if the complete dict only shows inf, create empty graph
        pass

    return response
